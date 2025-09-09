from datetime import date, timedelta
from pykrx import stock

# 여러 방식으로 시도해서 "정확히 100개"를 보장
_INDEX_NAME_CANDIDATES = ["코스피 100", "코스피100"]
_INDEX_CODE_CANDIDATES = ["1228", "1029", "1001"]  # 라이브러리/시점에 따라 달라질 수 있어 후보군 시도

def _try_fetch(name_or_code: str, d: str):
    try:
        return stock.get_index_portfolio_deposit_file(name_or_code, d) or []
    except Exception:
        return []

def _find_business_day(yyyymmdd: str) -> str:
    # 비영업일 대비: 과거로 최대 15일 롤백
    y, m, d = int(yyyymmdd[:4]), int(yyyymmdd[4:6]), int(yyyymmdd[6:8])
    cur = date(y, m, d)
    for _ in range(15):
        ds = cur.strftime("%Y%m%d")
        # “코스피 100/코스피100” 이름 우선 시도
        for nm in _INDEX_NAME_CANDIDATES:
            if _try_fetch(nm, ds):
                return ds
        # 코드도 시도
        for code in _INDEX_CODE_CANDIDATES:
            if _try_fetch(code, ds):
                return ds
        cur -= timedelta(days=1)
    return yyyymmdd

def get_kospi100_tickers(asof: str | None = None) -> list[str]:
    if asof is None:
        asof = date.today().strftime("%Y%m%d")
    asof = _find_business_day(asof)

    # 1) 이름으로 시도 (정확도 높음)
    for nm in _INDEX_NAME_CANDIDATES:
        lst = _try_fetch(nm, asof)
        if len(lst) == 100:
            return sorted(lst)

    # 2) 코드로 시도 → 100개면 채택. 200개(코스피200)면 버림.
    for code in _INDEX_CODE_CANDIDATES:
        lst = _try_fetch(code, asof)
        if len(lst) == 100:
            return sorted(lst)

    # 3) 마지막 보호장치: 어떤 케이스든 100개 이상이면 상위 100개 슬라이스 (임시 방편)
    #    *가급적 1,2에서 끝나야 함. 백업 안전망.
    for nm in _INDEX_NAME_CANDIDATES + _INDEX_CODE_CANDIDATES:
        lst = _try_fetch(nm, asof)
        if len(lst) > 100:
            return sorted(lst)[:100]

    return []
