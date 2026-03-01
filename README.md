# 1️⃣ Project Overview
📌 프로젝트 목적

KOSPI 주가 데이터를 자동 수집·적재·예측·평가하는 배치 파이프라인 구축

수작업 분석을 자동화하고 재현 가능한 데이터 처리 환경 설계

# 2️⃣ Architecture
[Data Source API]
        ↓
[Ingestion]
        ↓
[PostgreSQL (Raw)]
        ↓
[Feature Engineering]
        ↓
[Prediction Model]
        ↓
[Evaluation]
        ↓
[Report CSV]
        ↓
[Streamlit Dashboard]

# 3️⃣ Data Flow
🔹 Step 1 — Ingestion

API 데이터 수집

Raw 데이터 적재

🔹 Step 2 — Feature Engineering

이동평균 등 파생변수 생성

결측치 처리

🔹 Step 3 — Prediction

ML/DL 모델 예측 수행

예측 결과 저장

🔹 Step 4 — Evaluation

실제 값 대비 오차 계산

평가 테이블 분리 저장

🔹 Step 5 — Reporting

CSV 리포트 자동 생성

Streamlit 시각화 반영

# 4️⃣ Database Design
📌 핵심 테이블

prices

predictions

prediction_eval

CREATE TABLE predictions (
    ticker VARCHAR(10),
    date DATE,
    predicted_price FLOAT,
    PRIMARY KEY (ticker, date)
);

# 5️⃣ Automation
Airflow DAG 기반 배치 실행

ingest → predict → eval → report 의존성 구성

Docker Compose 통합 환경 구축

재현 가능한 실행 구조

# 6️⃣ Issues & Improvements
JDBC 드라이버 인식 문제 → 컨테이너 내 드라이버 경로 수정

DB 연결 타이밍 이슈 → DAG 의존성 재구성

중복 예측 데이터 문제 → upsert 전략 도입

# 7️⃣ Results
가격 데이터 적재: 276,738 rows (prices)

예측 결과 저장: 1,922,525 rows (predictions)

평가 결과 저장: 1,921,679 rows (prediction_eval)

Airflow DAG를 통한 end-to-end 실행 검증 완료

<img width="820" height="554" alt="스크린샷 2026-03-01 172447" src="https://github.com/user-attachments/assets/841e8adb-a155-4246-83de-e278655465df" />


# 8️⃣ What I Learned
데이터 파이프라인 설계 중요성

자동화와 재현성의 가치

DB 정합성 관리 경험
