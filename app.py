import requests
from bs4 import BeautifulSoup
import pandas as pd
import time
import random
import re
from io import BytesIO
from urllib.parse import quote_plus, urlparse
from datetime import datetime, timedelta, timezone

try:
    import streamlit as st
except ImportError:  # pragma: no cover - streamlit이 없는 환경 대비
    st = None

# =========================
# 기본 설정
# =========================
DEFAULT_SEARCH_TERM = '"여의시스템"'    # 쌍따옴표 포함 (정확 일치)
DEFAULT_START_DATE = "2024.01.01"
DEFAULT_END_DATE = "2024.12.31"
DEFAULT_SORT = 2                       # 0:관련도, 1:최신순, 2:오래된순
DEFAULT_MAX_PAGES = 200                # 안전장치
DEFAULT_OUTPUT_XLSX = "naver_news_search_results_여의시스템_v1_2024.xlsx"
DATE_FORMAT = "%Y.%m.%d"

SORT_OPTIONS = {
    "관련도": 0,
    "최신순": 1,
    "오래된순": 2,
}

SORT_LABEL_BY_VALUE = {value: label for label, value in SORT_OPTIONS.items()}

# 시간대 (KST)
KST = timezone(timedelta(hours=9))

# 공통 요청 헤더
headers = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/124.0.0.0 Safari/537.36"
    ),
    "Accept-Language": "ko-KR,ko;q=0.9,en-US;q=0.8,en;q=0.7",
    "Referer": "https://www.naver.com/",
}

# 세션 + 가벼운 재시도
session = requests.Session()
adapter = requests.adapters.HTTPAdapter(max_retries=2)
session.mount("http://", adapter)
session.mount("https://", adapter)

# 날짜/상대시간 패턴
DATE_PAT = re.compile(r'20\d{2}\.\d{1,2}\.\d{1,2}')
REL_PAT  = re.compile(r'(\d+)\s*(분|시간|일)\s*전')

# headline 전용 선택자 (제목 뽑기용)
HEADLINE_SEL = (
    "span.sds-comps-text.sds-comps-text-ellipsis-1.sds-comps-text-type-headline1, "
    "span.sds-comps-text.sds-comps-text-ellipsis-2.sds-comps-text-type-headline1, "
    "span.sds-comps-text.sds-comps-text-type-headline1, "
    "span.sds-comps-text.sds-comps-text-type-headline2"
)

# 언론사 전용 선택자
PRESS_SEL = (
    # 스크린샷 구조: 프로필(언론사) 타이틀 영역
    "div.sds-comps-profile-info-title "
    "span.sds-comps-text.sds-comps-text-type-body2.sds-comps-profile-info-title-text, "
    "div.sds-comps-profile-info-title a > "
    "span.sds-comps-text.sds-comps-text-type-body2"
)

def build_url(encoded_query: str, sort_value: int, start_date: str, end_date: str, nso_period: str, start: int) -> str:
    """네이버 뉴스 검색 URL 생성(where=news 레이아웃)"""
    return (
        "https://search.naver.com/search.naver?"
        f"where=news&sm=tab_pge&sort={sort_value}&photo=0&field=0"
        f"&query={encoded_query}"
        "&pd=3"
        f"&ds={start_date}&de={end_date}"
        f"&nso=so%3Ar%2Cp%3A{nso_period}"
        f"&start={start}"
    )

def _normalize_relative_date(text: str) -> str:
    """
    '3시간 전', '2일 전', '45분 전', '어제', '오늘' → 'YYYY.MM.DD' (KST 기준)
    """
    t = text.strip()
    now = datetime.now(KST)
    m = REL_PAT.search(t)
    if m:
        n, unit = int(m.group(1)), m.group(2)
        if unit == "분":
            dt = now - timedelta(minutes=n)
        elif unit == "시간":
            dt = now - timedelta(hours=n)
        else:
            dt = now - timedelta(days=n)
        return dt.strftime("%Y.%m.%d")
    if "어제" in t:
        return (now - timedelta(days=1)).strftime("%Y.%m.%d")
    if "오늘" in t:
        return now.strftime("%Y.%m.%d")
    return ""

def _extract_date_from_url(url: str) -> str:
    """URL 경로에 /2025/07/01/ 또는 /20250701/ 같은 날짜가 박힌 경우 추출"""
    try:
        m = re.search(r'/(20\d{2})(\d{2})(\d{2})/', url)
        if m:
            y, mo, d = m.groups()
            return f"{y}.{mo}.{d}"
        m = re.search(r'/(20\d{2})/(\d{1,2})/(\d{1,2})/', url)
        if m:
            y, mo, d = m.groups()
            return f"{y}.{int(mo):02d}.{int(d):02d}"
        m = re.search(r'/(20\d{2})(\d{2})(\d{2})', url)
        if m:
            y, mo, d = m.groups()
            return f"{y}.{mo}.{d}"
    except:
        pass
    return ""

def _extract_date_from_article(url: str) -> str:
    """
    최후의 보루: 기사 원문 페이지에서 <time datetime>, og:article:published_time 등으로 추출
    (느려지므로 정말 필요할 때만 호출)
    """
    try:
        r = session.get(url, headers=headers, timeout=8)
        if r.status_code != 200:
            return ""
        html = r.text
        m = re.search(r'<time[^>]*datetime=["\']([^"\']+)["\']', html, flags=re.I)
        if m:
            iso = m.group(1)
            try:
                dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                return dt.astimezone(KST).strftime("%Y.%m.%d")
            except:
                pass
        m = re.search(
            r'property=["\']article:published_time["\'][^>]*content=["\']([^"\']+)["\']',
            html, flags=re.I
        )
        if m:
            iso = m.group(1)
            try:
                dt = datetime.fromisoformat(iso.replace("Z", "+00:00"))
                return dt.astimezone(KST).strftime("%Y.%m.%d")
            except:
                pass
        m = re.search(r'(\d{4})[.\-/](\d{1,2})[.\-/](\d{1,2})', html)
        if m:
            y, mo, d = m.groups()
            return f"{y}.{int(mo):02d}.{int(d):02d}"
    except:
        pass
    return ""

def _clean_text(s: str) -> str:
    return " ".join(s.replace("\n", " ").replace("\r", " ").split())

def _extract_title_from_card(box) -> str:
    """
    sds 뉴스 카드 구조에서 제목 전용 span(headline1/2)을 우선 추출.
    mark 태그가 섞여 있어도 get_text(' ', strip=True)로 자연스럽게 합쳐짐.
    """
    # 1) headline 전용 span
    span = box.select_one(HEADLINE_SEL)
    if span:
        return _clean_text(span.get_text(" ", strip=True))

    # 2) headline span을 자식으로 가진 a
    a = box.select_one(f"a:has(> {HEADLINE_SEL})")
    if a:
        sp = a.select_one(HEADLINE_SEL)
        if sp:
            return _clean_text(sp.get_text(" ", strip=True))

    # 3) 공용 구조(뉴스 타이틀)
    a = box.select_one("a.news_tit") or box.find("a", attrs={"class": re.compile("news_tit")})
    if a:
        return _clean_text(a.get_text(" ", strip=True))

    # 4) 보조: 키워드 포함 긴 a 텍스트
    for x in box.find_all("a"):
        txt = _clean_text(x.get_text(" ", strip=True))
        if "여의시스템" in txt and len(txt) > 10:
            return txt
    return ""

def _extract_source_from_card(box, link: str = "") -> str:
    """카드 DOM에서 언론사명을 최대한 안정적으로 추출."""
    # 1) 신규/sds 구조 우선
    sp = box.select_one(PRESS_SEL)
    if sp:
        src = _clean_text(sp.get_text(strip=True))
        if src:
            return src

    # 2) 공용/구형 네이버 구조
    a_press = box.select_one("a.info.press") or box.select_one("div.info_group > a.info")
    if a_press:
        src = _clean_text(a_press.get_text(strip=True))
        if src:
            return src

    # 3) 클래스 키워드 기반 보조
    cand = box.find(class_=re.compile(r"(press|source|profile-info-title)", re.I))
    if cand:
        src = _clean_text(cand.get_text(strip=True))
        if src and len(src) <= 30:
            return src

    # 4) 최후의 보루: 도메인
    if link:
        try:
            domain = urlparse(link).netloc.replace("www.", "")
            if domain:
                return domain.split(".")[0]
        except:
            pass
    return ""

def parse_page(html: str):
    """
    우선순위:
    1) 안정 구조: ul.list_news > li.bx 안의 a.news_tit, div.info_group > span.info
    2) 보조 구조: sds-comps-...(headline/date/press)
    3) 최후의 보루: 기사 원문에서 date 메타 추출
    """
    soup = BeautifulSoup(html, "lxml")  # lxml 권장

    rows = []
    # 1) 가장 안정적인 구조
    boxes = soup.select("ul.list_news > li.bx")
    # 2) 없다면 sds 계열 보조
    if not boxes:
        boxes = soup.select("div.news_area, div.sds-comps-base-layout, div.sds-comps-vertical-layout")

    for box in boxes:
        # 제목 (headline 우선)
        title = _extract_title_from_card(box)
        if not title:
            continue

        # 링크: headline span을 자식으로 가진 a → 공용 a.news_tit → 첫 a
        a = box.select_one(f"a:has(> {HEADLINE_SEL})")
        if not a:
            a = box.select_one("a.news_tit") or box.find("a", attrs={"class": re.compile("news_tit")})
        if not a:
            a = box.find("a")
        if not a:
            continue
        link = a.get("href", "").strip()
        if not link:
            continue

        # 언론사 (전용 함수)
        source = _extract_source_from_card(box, link)

        # 날짜
        date_text = ""

        # (a) 안정 구조: info_group > span.info
        for sp in box.select("div.info_group > span.info"):
            t = _clean_text(sp.get_text(strip=True))
            m = DATE_PAT.search(t)
            if m:
                date_text = m.group()
                break
            rel = _normalize_relative_date(t)
            if rel:
                date_text = rel
                break

        # (b) sds 세 클래스 AND 매칭
        if not date_text:
            for sp in box.select("span.sds-comps-text.sds-comps-text-type-body2.sds-comps-text-weight-sm"):
                t = _clean_text(sp.get_text(strip=True))
                m = DATE_PAT.search(t)
                if m:
                    date_text = m.group()
                    break
                rel = _normalize_relative_date(t)
                if rel:
                    date_text = rel
                    break

        # (c) time 태그
        if not date_text:
            ttag = box.find("time")
            if ttag:
                dt_attr = ttag.get("datetime")
                if dt_attr:
                    try:
                        dt = datetime.fromisoformat(dt_attr.replace("Z", "+00:00"))
                        date_text = dt.astimezone(KST).strftime("%Y.%m.%d")
                    except:
                        pass
                if not date_text:
                    t = _clean_text(ttag.get_text(strip=True))
                    m = DATE_PAT.search(t)
                    if m:
                        date_text = m.group()
                    else:
                        rel = _normalize_relative_date(t)
                        if rel:
                            date_text = rel

        # (d) URL 기반 추출
        if not date_text and link:
            date_text = _extract_date_from_url(link)

        # (e) 최후의 보루: 기사 원문
        if not date_text and link:
            date_text = _extract_date_from_article(link)

        rows.append({
            "Title":  title,
            "Date":   date_text,
            "Source": source,
            "Link":   link
        })

    return rows

def crawl(
    search_term: str,
    start_date: str,
    end_date: str,
    sort_value: int,
    max_pages: int,
    sleep_range: tuple[float, float] = (1.0, 2.0),
    log=print,
):
    """
    네이버 뉴스 검색 결과를 크롤링하여 (제목, 날짜, 언론사, 링크) 목록을 반환합니다.
    log 매개변수에 콜러블을 전달해 진행 상황을 스트림릿 등에 출력할 수 있습니다.
    """
    titles, dates, sources, links = [], [], [], []
    seen_links = set()

    encoded_term = quote_plus(search_term)
    nso_period = f"from{start_date.replace('.', '')}to{end_date.replace('.', '')}"
    start = 1  # 1, 11, 21 ...
    page_count = 0
    sleep_min, sleep_max = sleep_range
    if sleep_min > sleep_max:
        sleep_min, sleep_max = sleep_max, sleep_min

    while True:
        page_count += 1
        if page_count > max_pages:
            log(f"안전 종료: max_pages({max_pages}) 초과")
            break

        url = build_url(encoded_term, sort_value, start_date, end_date, nso_period, start)
        log(f"[요청] {url}")
        try:
            resp = session.get(url, headers=headers, timeout=10)
            if resp.status_code != 200:
                log(f"요청 실패(status={resp.status_code}) start={start}")
                break
        except requests.RequestException as e:
            log(f"요청 예외: {e}")
            break

        rows = parse_page(resp.text)

        if not rows:
            with open("debug_naver.html", "w", encoding="utf-8") as f:
                f.write(resp.text)
            log(f"페이지 start={start} 기사 없음. debug_naver.html 확인")
            break

        new_cnt = 0
        for r in rows:
            link = r["Link"]
            if not link or link in seen_links:
                continue
            seen_links.add(link)
            titles.append(r["Title"])
            dates.append(r["Date"])
            sources.append(r["Source"])
            links.append(link)
            new_cnt += 1

        log(f"페이지(start={start}) 수집 {len(rows)}건 / 신규 {new_cnt}건")

        # 다음 페이지 (네이버는 10단위 페이지네이션: 1, 11, 21, ...)
        start += 10

        # 예의 있는 대기
        wait_time = random.uniform(sleep_min, sleep_max)
        time.sleep(wait_time)

        # 신규가 하나도 없으면 조기 종료
        if new_cnt == 0:
            log("신규 항목 없음 → 조기 종료")
            break

    return titles, dates, sources, links


def build_dataframe(titles, dates, sources, links) -> pd.DataFrame:
    return pd.DataFrame({
        "Title": titles,
        "Date": dates,
        "Source": sources,
        "Link": links,
    })


def streamlit_main():
    if st is None:
        raise RuntimeError("Streamlit이 설치되어 있지 않습니다. `pip install streamlit` 후 다시 실행해주세요.")

    st.set_page_config(page_title="네이버 뉴스 검색 수집기", layout="wide")
    st.title("네이버 뉴스 검색 수집기")
    st.markdown("네이버 뉴스에서 특정 키워드의 기사를 기간별로 수집하고 엑셀로 저장할 수 있습니다.")

    default_start_date = datetime.strptime(DEFAULT_START_DATE, DATE_FORMAT).date()
    default_end_date = datetime.strptime(DEFAULT_END_DATE, DATE_FORMAT).date()
    sort_labels = list(SORT_OPTIONS.keys())
    default_sort_label = SORT_LABEL_BY_VALUE.get(DEFAULT_SORT, sort_labels[0])
    default_sort_index = sort_labels.index(default_sort_label)

    with st.form("search_form"):
        search_term = st.text_input("검색어", value=DEFAULT_SEARCH_TERM)
        col1, col2 = st.columns(2)
        with col1:
            start_date_input = st.date_input("시작일", value=default_start_date, format="YYYY.MM.DD")
        with col2:
            end_date_input = st.date_input("종료일", value=default_end_date, format="YYYY.MM.DD")

        sort_label = st.selectbox("정렬 기준", sort_labels, index=default_sort_index)
        max_pages = st.number_input("최대 페이지 수 (페이지당 10건)", min_value=1, max_value=1000, value=DEFAULT_MAX_PAGES, step=1)
        output_filename = st.text_input("엑셀 파일명", value=DEFAULT_OUTPUT_XLSX)
        save_to_disk = st.checkbox("로컬 파일로 저장", value=True)
        submitted = st.form_submit_button("수집 시작")

    if not submitted:
        return

    if start_date_input > end_date_input:
        st.error("시작일이 종료일보다 늦을 수 없습니다.")
        return

    start_date_str = start_date_input.strftime(DATE_FORMAT)
    end_date_str = end_date_input.strftime(DATE_FORMAT)
    sort_value = SORT_OPTIONS[sort_label]

    status_placeholder = st.empty()
    log_messages = []

    def log_to_streamlit(msg: str):
        timestamp = datetime.now().strftime("%H:%M:%S")
        log_messages.append(f"[{timestamp}] {msg}")
        status_placeholder.text("\n".join(log_messages[-10:]))

    with st.spinner("크롤링 중입니다. 잠시만 기다려주세요..."):
        try:
            titles, dates, sources, links = crawl(
                search_term=search_term,
                start_date=start_date_str,
                end_date=end_date_str,
                sort_value=sort_value,
                max_pages=int(max_pages),
                log=log_to_streamlit,
            )
        except Exception as exc:  # pragma: no cover - 주로 네트워크 에러 대비
            st.error(f"크롤링 중 오류가 발생했습니다: {exc}")
            return

    if not titles:
        st.warning("수집된 기사가 없습니다.")
        return

    df = build_dataframe(titles, dates, sources, links)
    st.success(f"총 {len(df)}개의 뉴스 기사를 수집했습니다.")
    st.dataframe(df, use_container_width=True)

    buffer = BytesIO()
    df.to_excel(buffer, index=False)
    buffer.seek(0)
    st.download_button(
        "엑셀 다운로드",
        buffer,
        file_name=output_filename or "naver_news_search_results.xlsx",
        mime="application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
    )

    if save_to_disk and output_filename:
        try:
            df.to_excel(output_filename, index=False)
            st.info(f"엑셀 파일이 저장되었습니다: `{output_filename}`")
        except Exception as exc:
            st.warning(f"로컬 파일 저장에 실패했습니다: {exc}")


def run_cli():
    titles, dates, sources, links = crawl(
        search_term=DEFAULT_SEARCH_TERM,
        start_date=DEFAULT_START_DATE,
        end_date=DEFAULT_END_DATE,
        sort_value=DEFAULT_SORT,
        max_pages=DEFAULT_MAX_PAGES,
    )

    if not titles:
        print("수집 결과가 없습니다.")
        return

    df = build_dataframe(titles, dates, sources, links)
    df.to_excel(DEFAULT_OUTPUT_XLSX, index=False)
    print(f"Data saved to {DEFAULT_OUTPUT_XLSX}")
    print(f"총 {len(df)}개의 뉴스 기사를 수집했습니다.")


def is_running_with_streamlit() -> bool:
    if st is None:
        return False
    if getattr(st, "_is_running_with_streamlit", False):
        return True
    try:
        from streamlit.runtime.scriptrunner import get_script_run_ctx

        return get_script_run_ctx() is not None
    except Exception:  # pragma: no cover - streamlit 내부 구조 변동 대비
        return False

if __name__ == "__main__":
    if is_running_with_streamlit():
        streamlit_main()
    else:
        run_cli()
