# 🏏 CricketPulse — Real-time IPL Data Engineering Platform

![Status](https://img.shields.io/badge/Status-Active-green)
![Python](https://img.shields.io/badge/Python-3.x-blue)
![Kafka](https://img.shields.io/badge/Apache_Kafka-7.4-black)
![Spark](https://img.shields.io/badge/PySpark-3.x-orange)
![License](https://img.shields.io/badge/License-MIT-yellow)

End-to-end Lambda architecture pipeline that ingests live IPL ball-by-ball 
events via Apache Kafka, processes with PySpark Structured Streaming, stores 
in Apache Iceberg data lake, orchestrated by Airflow, and visualized on 
a live Grafana dashboard.

> **Built as part of a 30-day Data Engineering learning journey**

## 🏗️ Architecture

```
Cricsheet JSON

↓

Python Producer → Apache Kafka (topic: ipl-balls)

↓

PySpark Structured Streaming → Apache Iceberg (data lake)

↓

Airflow DAG (daily batch) → PostgreSQL (warehouse)

↓

FastAPI + Grafana Dashboard
```
## ⚡ Stack
| Layer | Tools |
|-------|-------|
| Ingestion | Python, Apache Kafka |
| Processing | PySpark Structured Streaming |
| Storage | Apache Iceberg, PostgreSQL |
| Orchestration | Apache Airflow |
| Quality | Great Expectations |
| Serving | FastAPI, Grafana |
| DevOps | Docker Compose, GitHub Actions |

## 🚀 Quick Start

```bash
git clone https://github.com/Rahul9672/cricket-pulse
cd cricket-pulse
docker-compose up -d
python3 ingestion/kafka_producer.py
```

## 📊 Live Output (Day 1 — Kafka verified)

```json
{"match_id": "2024-04-09", "batting_team": "Sunrisers Hyderabad",
 "over": 0, "ball": 3, "batter": "TM Head", "bowler": "K Rabada",
 "runs_off_bat": 4, "extras": 0, "total_runs": 4, "wicket": null}
```
> 251 ball events published and verified in Kafka topic `ipl-balls` ✅

## 📈 Build Progress
- [x] Day 1: Cricsheet parser + Kafka producer (251 events verified ✅)
- [ ] Day 2: PySpark Structured Streaming consumer
- [ ] Day 3: Apache Iceberg data lake
- [ ] Day 4: Airflow orchestration
- [ ] Day 5: Grafana dashboard

## 📂 Project Structure

cricket-pulse/

├── ingestion/

│   ├── kafka_producer.py      ← live match simulator

│   └── cricsheet_parser.py    ← JSON parser

├── streaming/                 ← PySpark jobs (Day 2)

├── batch/                     ← Airflow DAGs (Day 4)

├── serving/                   ← FastAPI (Day 5)

├── quality/                   ← Great Expectations

├── dashboard/                 ← Grafana config

└── docker-compose.yml         ← one command setup


## 🙋 Author
**Rahul Kumar** — Backend Developer → Data Engineer  
B.Tech Geoinformatics, NSUT Delhi  
[LinkedIn](https://linkedin.com/in/your-link) · [GitHub](https://github.com/Rahul9672)