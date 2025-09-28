# KOSPI Portfolio ETL with Airflow + DL/ML + Streamlit

주식(KOSPI) 데이터를 **매일 자동 수집 → 예측(DL/ML) → 평가 → 리포트 CSV 생성**까지 수행하는 파이프라인입니다.  
또한 저장된 데이터를 기반으로 **Streamlit 대시보드**에서 시각화를 제공합니다.

## 구성 개요

project/

├─ airflow/

│ ├─ dags/

│ │ └─ airflow_etl_daily.py # DAG: refresh → incremental → predict → eval → report

│ └─ docker-compose.yml # Airflow 스택

├─ sql/

│ └─ schema.sql # DB 스키마 (tickers, prices, predictions, prediction_eval)

├─ src/

│ ├─ db/conn.py # DB 엔진 팩토리

│ ├─ ingest/

│ │ ├─ refresh_tickers.py # 티커/종목명 갱신

│ │ └─ incremental_prices.py # 신규 주가만 증분 수집

│ ├─ models/

│ │ ├─ baseline_safe.py # MA/SES 안전 예측

│ │ └─ dl_lstm.py # LSTM 모델 예측(실동작)

│ └─ pipeline/

│ ├─ predict_daily.py # 모델 예측 → predictions UPSERT

│ ├─ ensemble_and_eval.py # 앙상블 + 성능평가 저장

│ └─ signals_report_daily.py # 리포트 CSV 생성

├─ app.py # Streamlit 시각화

├─ requirements.txt

├─ .env # 환경변수(비공개)

└─ README.md

perl
코드 복사

## 주요 테이블/뷰

- **tickers**(ticker PK, name, market, sector, updated_at)
- **prices**(date, ticker PK, open/high/low/close/volume/change, name)
- **predictions**(date, ticker, model_name, horizon, y_pred, **PK(date,ticker,model_name,horizon)**)
- **prediction_eval**(date, ticker, model_name, horizon, y_true, abs_err, dir_correct, **PK(...)**)
- (선택) **predictions_clean 뷰**: `model_name LIKE 'safe_%' AND horizon=1` 만 노출

## 요구사항

- Docker / Docker Compose
- Python 3.10+ (개발·로컬 실행용), Airflow 컨테이너 내부 Python 3.12
- PostgreSQL (Docker 컨테이너 포함)
- 주요 파이썬 패키지: `apache-airflow==2.9.2`, `sqlalchemy`, `psycopg2`, `pandas`,  
  `FinanceDataReader`, `pykrx`, `statsmodels`, `tensorflow`(DL), `streamlit`

## 환경변수 (`.env`)

> `.env`는 **커밋 금지**(.gitignore로 제외)  
> 예시:

Business DB (주가/예측 저장)
DB_HOST=host.docker.internal
DB_PORT=5432
DB_NAME=stocks
DB_USER=kospi
DB_PASS=kospi
DB_URL=postgresql+psycopg2://kospi:kospi@host.docker.internal:5432/stocks

Airflow Metadata DB
AIRFLOW_DB_HOST=airflow-postgres
AIRFLOW_DB_PORT=5432
AIRFLOW_DB_NAME=airflow
AIRFLOW_DB_USER=airflow
AIRFLOW_DB_PASS=airflow

AIRFLOW_UID=50000
AIRFLOW__CORE__LOAD_EXAMPLES=False
AIRFLOW__CORE__EXECUTOR=LocalExecutor
TZ=Asia/Seoul

Airflow keys
AIRFLOW__WEBSERVER__SECRET_KEY=<your_web_secret>
AIRFLOW__CORE__FERNET_KEY=<your_fernet_key>

extra requirements
PIP_ADDITIONAL_REQUIREMENTS=finance-datareader pykrx statsmodels python-dotenv tensorflow


## 설치 & 실행

### 1) DB 스키마 적용

```bash
# 컨테이너 이름 예: pg-kospi
docker cp sql/schema.sql pg-kospi:/schema.sql
docker exec -it pg-kospi psql -U kospi -d stocks -f /schema.sql
스키마에는 PK/Unique 제약 및 필요한 테이블/뷰가 포함됩니다.

2) Airflow 스택 기동
cd airflow
docker compose up -d --force-recreate
웹 UI: http://localhost:8080 (기본 계정은 airflow-init에서 생성하도록 compose 구성)

3) DAG 구조
DAG ID: airflow_etl_daily

refresh_tickers: 코스피200(지수코드 1028) → ticker/이름 갱신 (pykrx, FDR fallback)

incremental_prices: 티커별 DB 마지막 일자 이후만 증분 수집

predict_daily: 안전모델(MA/SES) + DL(LSTM) 예측값 저장(upsert)

ensemble_and_eval: 앙상블(safe_ens_mean/median) + 메트릭 저장

signals_report_daily: 리포트 CSV 생성(영업일/휴장일 로직 포함)

스케줄: 평일 06:00 KST (예시)

4) 수동 실행 (빠른 점검용)
# 컨테이너에서 직접
docker exec -it airflow-airflow-scheduler-1 bash -lc "cd /opt/project && export PYTHONPATH=/opt/project && python -m src.ingest.refresh_tickers"
docker exec -it airflow-airflow-scheduler-1 bash -lc "cd /opt/project && export PYTHONPATH=/opt/project && python -m src.ingest.incremental_prices"
docker exec -it airflow-airflow-scheduler-1 bash -lc "cd /opt/project && export PYTHONPATH=/opt/project && python -m src.pipeline.predict_daily --limit 20"
docker exec -it airflow-airflow-scheduler-1 bash -lc "cd /opt/project && export PYTHONPATH=/opt/project && python -m src.pipeline.ensemble_and_eval --limit 20"
docker exec -it airflow-airflow-scheduler-1 bash -lc "cd /opt/project && export PYTHONPATH=/opt/project && python -c 'from src.pipeline.signals_report_daily import run; run()'"
DL 속도/리소스가 걱정되면 --limit로 티커 수를 제한해서 테스트하세요.

5) Streamlit 대시보드
# 로컬 가상환경 (DL 설치 포함)에서
cd C:\Users\user\project
streamlit run app.py
app.py는 DB와 tickers를 조인해 티커명/모델별 최신 예측/신호를 시각화합니다.

내부 쿼리는 predictions_clean(선택적 뷰) 또는 predictions를 사용하도록 설정 가능.

운영 포인트
증분 수집: prices에서 티커별 MAX(date)를 읽고 그 다음 영업일부터 API 호출 → 저장

중복 방지: prices PK (date,ticker), predictions PK (date,ticker,model_name,horizon)
UPSERT(ON CONFLICT … DO UPDATE) 로 재실행 안전

휴장일 처리: signals_report_daily.py에서 휴장일/주말 로직으로 적절히 기준일 결정

성능: 전체 티커 예측은 시간이 걸릴 수 있음 → 캐시/인덱스/샤딩/배치크기 등 개선 여지

장애 복구: DB collation 경고는 기능에는 영향 없지만, 동일 OS/locale로 재구성 시 해소 가능

트러블슈팅 체크리스트
DB 연결 실패: 컨테이너에서 printenv DB_HOST DB_PORT DB_NAME DB_USER DB_PASS 확인

src 모듈 인식 오류: DAG bash_command 에 export PYTHONPATH=/opt/project 포함 여부 확인

ON CONFLICT 에러: predictions의 PK/유니크 인덱스 존재 확인

DL 에러: tensorflow 설치/런타임 에러 로그 확인, 메모리/버전 호환 체크
