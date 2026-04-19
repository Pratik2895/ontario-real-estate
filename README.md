# 🏗️ Ontario Real Estate Lakehouse

> Production-grade Medallion Architecture pipeline for Ontario real estate analytics — **1.2M+ civic records** processed through Bronze, Silver, and Gold layers with a Streamlit dashboard on top.

![PySpark](https://img.shields.io/badge/PySpark-3.5-E25A1C?style=flat-square&logo=apachespark&logoColor=white)
![Delta Lake](https://img.shields.io/badge/Delta%20Lake-3.x-00ADD4?style=flat-square)
![Databricks](https://img.shields.io/badge/Databricks-FF3621?style=flat-square&logo=databricks&logoColor=white)
![Streamlit](https://img.shields.io/badge/Streamlit-FF4B4B?style=flat-square&logo=streamlit&logoColor=white)
![License](https://img.shields.io/badge/License-MIT-green?style=flat-square)

🔗 **Portfolio case study →** https://pratik2895.github.io/projects/ontario-real-estate/

---

## 🎯 The problem

Toronto and the Ontario government publish valuable real estate data — property boundaries, building permits, rental evaluations, housing price indices — but it's fragmented across portals, inconsistently shaped, and updated on different cadences. No single trustworthy view for decision-makers.

## 🧱 The solution — Medallion (Bronze / Silver / Gold)

```
                  ┌─────────┐   ┌─────────┐   ┌─────────┐
 Toronto Open  ──►│ Bronze  │──►│ Silver  │──►│  Gold   │──► Streamlit
 Data + StatsCan  │ 8 tables│   │ 4 tables│   │ 6 tables│    dashboard
                  └─────────┘   └─────────┘   └─────────┘
                     raw         cleaned       business
                  landing        + dedup       aggregates
```

| Layer | Tables | Purpose |
|---|---|---|
| 🥉 **Bronze** | 8 | Raw API landing, idempotent ingestion, audit columns |
| 🥈 **Silver** | 4 | Type-cast, deduplicated, enriched with geospatial keys, schema-enforced Delta writes |
| 🥇 **Gold** | 6 | Business-ready aggregates — permit trends, construction investment, apartment quality scores, price indices |

---

## 📊 At a glance

| Metric | Value |
|---|---|
| Records processed | **1.2M+** |
| Source data volume | **540 MB** |
| Gold tables | **6** |
| Dashboard tabs | **6** — permits, construction, housing dev, apartment quality, price index, overview |

---

## 🧰 Tech stack

- **Compute:** PySpark on Databricks
- **Storage:** Delta Lake
- **Orchestration:** Databricks Workflows
- **Serving:** Streamlit dashboard
- **Sources:** Toronto Open Data APIs · Statistics Canada housing price indices

---

## 🚀 Run locally

```bash
# 1. Clone
git clone https://github.com/Pratik2895/ontario-real-estate
cd ontario-real-estate

# 2. Install
pip install -r requirements.txt

# 3. Run the pipeline (Databricks or local Spark)
python src/pipeline/run.py

# 4. Launch the dashboard
streamlit run streamlit_app/app.py
```

---

## 🧭 Repo structure

```
.
├── src/
│   ├── bronze/          # Raw ingestion jobs
│   ├── silver/          # Cleaning + dedup
│   ├── gold/            # Aggregations
│   └── shared/          # Schema, utils
├── streamlit_app/       # Dashboard
├── data/                # Sample data (git-ignored)
├── tests/
└── README.md
```

---

## 📈 Why this matters

This project is my **public playground for production patterns**: incremental Delta writes, schema enforcement, layered refinement, idempotent reruns, and a thin-but-real serving UI. The same patterns I ship internally at Intuit — just on open civic data.

---

## 📫 Contact

**Pratik Bhikadiya** · Data & Analytics Engineer
[Portfolio](https://pratik2895.github.io) · [LinkedIn](https://www.linkedin.com/in/pratikbhikadiya/) · bhikadi@uwindsor.ca
