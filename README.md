# 📈 KOSPI100 주가 예측 데모 (PostgreSQL + Streamlit)

이 프로젝트는 **KOSPI100 주식 종가의 D+1 예측**을 목표로 하는 **실무형 데이터 파이프라인 + 웹 애플리케이션** 데모입니다.  
데이터 수집부터 정제, 예측, 앙상블, 평가, 그리고 시각화까지 **엔드투엔드(End-to-End)** 흐름을 제공합니다.

---

## 🚀 주요 기능

- **Pipeline**
  - Day1: 가격 데이터 수집 및 정제 → `prices`
  - Day2: 안전 베이스라인 예측(`safe_%`) → `predictions`
  - Day3: 앙상블(`safe_ens_*`) + 평가(`prediction_eval.dir_correct`)

- **Database**
  - 핵심 테이블: `prices`, `predictions`, `prediction_eval(dir_correct)`
  - 뷰(Views):  
    - `last250_dates`: 최근 250 거래일 기준  
    - `prediction_metrics`: 티커별 성능 요약 (MAE/ACC 전체·최근250)  
    - `prediction_leaderboard`: 모델별 전반 성능 요약  
    - `signals_view`: 예측 변화율 기반 시그널

- **Streamlit App**
  - 📈 티커별 성능: 실제 vs 예측, 모델 선택, 지표/CSV 다운로드
  - 🏆 모델 리더보드: `mae_250d` 우선 성능 비교 차트
  - 🔬 모델 비교: 티커 1개, 모델 2~3개 비교, 최근 250일 토글
  - 🚨 시그널 보드: 임계값 기반 Top N 시그널 탐지

---

## 📂 폴더 구조
project/
├─ sql/
│ └─ schema.sql # DB 테이블/뷰 통합 스키마
├─ src/
│ ├─ db/
│ │ └─ conn.py # DB 연결(SQLAlchemy)
│ ├─ pipeline/
│ │ ├─ day1_ingest_clean_load.py
│ │ ├─ day2_predict_baseline_safe.py
│ │ └─ day3_ensemble_and_eval.py
│ └─ web/
│ └─ app.py # Streamlit 앱 (최종본)
├─ .gitignore
├─ requirements.txt
└─ run.bat # venv 미활성 상태에서도 전체 실행

## ⚙️ 설치 및 준비

1. Python 가상환경 생성
```bat
python -m venv .venv
.\.venv\Scripts\activate

2. 패키지 설치
pip install -r requirements.txt

3. PostgreSQL 스키마 반영 (Docker 사용 예시)
docker exec -i pg-kospi psql -U kospi -d stocks < sql\schema.sql

▶️ 파이프라인 실행
.\.venv\Scripts\python.exe -m src.pipeline.day1_ingest_clean_load
.\.venv\Scripts\python.exe -m src.pipeline.day2_predict_baseline_safe
.\.venv\Scripts\python.exe -m src.pipeline.day3_ensemble_and_eval

🖥️ 앱 실행
.\.venv\Scripts\python.exe -m streamlit run src/web/app.py

📌 TODO / 발전 방향

워치리스트 기능 (Streamlit + DB)

모델 다양화 (ML/딥러닝 추가)

배포 자동화 (Docker Compose, GitHub Actions)

시각화 고도화 (Superset/Metabase 연동)
