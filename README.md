# 📈 KOSPI100 주가 예측 데모 (PostgreSQL + Streamlit)

데이터 엔지니어링 포트폴리오용 미니 프로젝트.  
- 데이터 적재 → 가공(SQL Views) → 시각화(Streamlit) → 공유(README)까지 **엔드 투 엔드** 흐름을 보여줍니다.

## 🚀 What's New (Day 4: 2025-09-11)
- **시그널 체계 분리**
  - 예측 기반: `signals_view` — 모델 `y_pred` 전일 대비 변화율/변화량 계산(탭4/탭5)  
  - 가격 기반: `signals_ma_view` — MA5/MA20 **골든/데드크로스** 발생일만 `BUY/SELL` (탭1 오버레이)
- **Streamlit 오버레이 추가 (탭1)**
  - Close/MA5/MA20 라인 위에 **▲BUY / ▼SELL** 마커 표시
  - 최근 1개월 토글과 연동된 구간 조회
- **캐시 최적화**
  - `@st.cache_data`(15분 기본)로 DB 부하 감소

## 🧱 아키텍처 (간단)
- **DB (PostgreSQL)**
  - 원본: `prices`, 예측: `predictions`, 평가: `prediction_eval`, 관심종목: `watchlist`
  - 앱 뷰:
    - `prediction_metrics`, `prediction_leaderboard`
    - `signals_view` (예측 변화율 기반)  
    - `signals_ma_view` (MA 교차 기반)
- **App (Streamlit)**
  - Tabs: `📈 티커별 성능`, `🏆 리더보드`, `🔬 모델 비교`, `🚨 시그널 보드`, `⭐ 관심 종목`
  - 오버레이: 탭1 하단 “시그널 오버레이 (MA 골든/데드크로스)”

## 📦 Quickstart
# 1) DB 스키마 반영
psql $DB_URL -f schema.sql

# 2) 앱 실행
pip install -r requirements.txt
streamlit run src/web/app.py


### 🔍 주요 SQL 뷰
signals_view (예측 기반): prediction_eval에서 LAG(y_pred)로 전일 대비 y_pred_pct_change, y_pred_abs_change 생성.
→ 탭4(임계값 슬라이더) / 탭5(관심종목 요약)에서 사용.

signals_ma_view (가격 기반): prices에서 MA5/MA20 윈도우 계산 → 교차 발생일만 BUY/SELL.
→ 탭1 오버레이에서 사용.

전체 정의는 schema.sql 참고.

### 🖥️ App 기능 요약
탭1: 단일 티커 실제 vs 예측 + MA 시그널 오버레이

탭2: 모델 리더보드 (전체/최근250 기준 MAE/ACC)

탭3: 모델 비교 (동일 티커, 다중 모델 라인 비교)

탭4: 시그널 보드 (임계값 |pct| 기준 상위 이벤트)

탭5: 관심 종목 관리(추가/삭제) + 빠른 차트/시그널 요약

탭6: 시그널 리포트 (예측 vs 가격)

### 🧩 구현 포인트(데이터 엔지니어 관점)
가공 책임을 DB(Views)로 이전 → 앱은 소비에 집중 (일관성/성능/테스트 용이)

신호 스키마 분리 (예측 vs 가격) → 의미 충돌 방지, 유지보수 쉬움

인덱스/윈도우 함수 활용 → 대용량에서도 확장성 고려

### 📸 스크린샷(추가 예정)
탭1: Close+MA+BUY/SELL 오버레이 화면

탭4: 임계값 슬라이더와 시그널 리스트

📝 Changelog
2025-09-11 (Day 4): 시그널 체계 분리, MA 오버레이/캐시 추가, README 업데이트

2025-09-10 (Day 3): Watchlist/리더보드/평가뷰 정비

🚀 What’s New (2025-09-13) — 데일리 증분 파이프라인
✅ 신규 스크립트

src/pipeline/ingest_daily.py

목적: DB prices에 **증분(마지막 적재일+1 ~ 오늘)**만 수집·정제·UPSERT

소스: FDR 우선 → 실패/빈DF 시 pykrx 폴백

대상: watchlist가 있으면 우선 사용, 없으면 KOSPI100 전체

멱등성: (ticker, date) ON CONFLICT UPSERT로 여러 번 실행해도 안전

옵션:

--since YYYY-MM-DD : 강제 시작일(백필/재처리용)

--tickers 005930,000660 : 특정 티커만

--dry-run : DB 미쓰기, 계획/품질만 확인

src/pipeline/predict_daily.py

목적: safe_* (MA5/10/20, SES a=0.3/0.5) 다음날 예측 생성 → predictions UPSERT

입력: 최신 prices

출력: predictions(date, ticker, model_name, horizon, y_pred)

src/pipeline/eval_daily.py

목적: as-of 예측을 **다음 거래일 실제값(y_true)**과 매칭 → prediction_eval UPSERT

지표: abs_err, dir_correct (상승/하락 방향 일치 여부)

▶️ 실행 순서 (가상환경 직접 호출 예시: Windows)
# 0) 스키마가 최신이 아닌 경우만
psql %DATABASE_URL% -f schema.sql

# 1) 증분 수집 (드라이런 → 실제)
.\.venv\Scripts\python.exe -m src.pipeline.ingest_daily --dry-run
.\.venv\Scripts\python.exe -m src.pipeline.ingest_daily

# 2) 데일리 예측
.\.venv\Scripts\python.exe -m src.pipeline.predict_daily

# 3) 데일리 평가
.\.venv\Scripts\python.exe -m src.pipeline.eval_daily

# 4) 앱 실행 (이미 설정된 경우 그대로)
streamlit run src/web/app.py


macOS/Linux는 ./.venv/bin/python -m ... 로 바꿔 실행하세요.

🧪 품질/정책 (요약)

중복 (date,ticker) 제거, date 타입 정리, volume < 0 필터

비거래일(주말/공휴일)은 소스에서 빈 DF가 오면 자동 스킵

일부 티커 실패해도 나머지는 계속 진행(티커 루프 단위 예외 처리)

🛠️ 버그 픽스

src/db/watchlist.py: 최신 종목명 CTE 명칭 오타 수정

latest_name → latest_names (LEFT JOIN 대상과 일치)

📊 앱 반영 포인트

탭2/탭3/탭4/탭6는 predictions/prediction_eval/signals_view 갱신을 자동 반영

오늘 파이프라인 실행 후, 리더보드·모델비교·시그널/리포트에서 업데이트 내용 확인 가능

⚠️ 참고(경고 메시지)

pykrx 실행 시 pkg_resources is deprecated 경고는 동작에 영향 없음(무시 가능)

📝 Changelog (append)

2025-09-13: 증분 수집/예측/평가 데일리 파이프라인 추가, watchlist 최신명 조인 수정

2025-09-11 (Day 4): 시그널 체계 분리, MA 오버레이/캐시 추가, README 업데이트

2025-09-10 (Day 3): Watchlist/리더보드/평가뷰 정비
