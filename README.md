# Greenhour Guardian: Smart EV Charging Pipeline

> An intelligent MLOps pipeline actively predicting the Danish grid's "dirty hours" to find the optimal window for EV charging.

## Our Mission: Protecting the Grid's Darkest Hours
Every evening between 17:00 and 21:00, the energy grid undergoes massive stress. Families come home, heaters turn on, and dinner starts cooking. When renewable energy like wind and solar cannot keep up with this sudden spike in demand, the grid panics and is forced to ignite highly polluting fossil-fuel "peaker plants."

If thousands of Electric Vehicles plug in during this exact window, it forces the grid to burn even more fossil fuels, defeating the purpose of driving a green car. **The Greenhour Guardian** was built to solve this. It actively predicts the grid's carbon intensity and aggressively warns users to **AVOID** charging during stress hours, mathematically hunting for the greenest window to quietly fuel the future.

## Why Exactly 6 Hours?
The pipeline is engineered to find a contiguous **6-hour charging block**. This is not an arbitrary number; it is based on European EV hardware standards:
* **Charger Output:** A standard European 3-phase home charger delivers **11 kW**.
* **Battery Capacity:** A standard EV battery (e.g., Tesla Model 3 SR, VW ID.4) is roughly **60-70 kWh**.
* **The Math:** 11 kW * 6 hours = **66 kWh** (A full 0-100% charge).
Giving the user less than 6 hours risks an uncharged car. Giving them more risks pushing charging into dirty, fossil-heavy hours.

---

## Pipeline Architecture
This project implements a complete, end-to-end MLOps lifecycle:

1. **Dataset Creation (`notebooks/`):** Compiled 2011-2026 data (ds, price_area, spot_price, co2, wind, solar) and hosted it on Hugging Face.
2. **Schema Definition (`schema.sql`):** Standardized rules to create and define the database tables.
3. **Database Engine (`connection.py`):** Establishes a secure SQLAlchemy connection to the Neon PostgreSQL database.
4. **Initialization (`initialize.py`):** Executes the schema to build the database infrastructure.
5. **Data Sync (`sync_data.py`):** Pulls the historical dataset from Hugging Face and aligns it into Neon.
6. **Live Ingestion (`ingest_job.py`):** Fetches recent weather, price, and CO2 data from APIs and stores it in the database.
7. **Model Training (`train_job.py`):** Engineers features, trains the geographically isolated XGBoost models (DK1 & DK2), and saves them to the DB.
8. **Forecasting (`predict_job.py`):** Predicts tomorrow's CO2, makes the 70/30 weighted recommendations, and stores the forecast.
9. **CI/CD Automation (`run_forecast.py`):** Triggers the prediction job and exports a `latest_forecast.json` file for the frontend.
10. **Evaluation (`evaluate_job.py`):** Evaluates model drift and performance metrics.
11. **User Interface (`app.py`):** A Streamlit application rendering the dark-mode recommendation dashboard.
12. **Containerization (`docker-compose.yml`):** Packages the entire environment for reproducible execution.

```mermaid
flowchart TD
    %% External Data Sources
    HF[Hugging Face Dataset] --> Sync
    API[Energi Data Service APIs] --> Ingest

    %% 0. Initial Setup (The Bootstrap)
    Sync[Initial Data Sync<br/><i>sync_data.py</i><br/>Loads Historical Data] --> DB[(Neon PostgreSQL<br/>Central Database)]

    %% 1. Ingestion Job
    CRON1((CRON 01:00<br/><i>ingest.yml</i>)) -->|Triggers| Ingest
    Ingest[Daily Ingestion<br/><i>ingest_job.py</i><br/>Pulls Yesterday's Actuals] --> DB

    %% 2. Evaluation Job
    CRON2((CRON 02:00<br/><i>evaluate.yml</i>)) -->|Triggers| Eval
    DB --> Eval[Model Evaluation<br/><i>evaluate_job.py</i><br/>Calculates Evidently Drift]
    Eval -. Saves Metrics .-> DB

    %% 3. Training Job
    CRON3((CRON 03:00 Sun<br/><i>train.yml</i>)) -->|Triggers| Train
    Eval -->|Drift > 0.3| Train
    DB --> Train[Model Training<br/><i>train_job.py</i><br/>Trains XGBoost & Features]
    Train -. Saves New Model .-> DB

    %% 4. Prediction Job
    CRON4((CRON 20:00<br/><i>predict.yml</i>)) -->|Triggers| Predict
    DB --> Predict[Daily Forecasting<br/><i>predict_job.py</i><br/>Predicts Tomorrow's CO2 & 70/30 Split]
    Predict -. Saves Forecast .-> DB

    %% Export and Frontend Delivery
    Predict --> Export[Export Data<br/><i>run_forecast.py</i>]
    Export --> JSON[/docs/latest_forecast.json/]
    
    JSON --> UI[Streamlit Frontend<br/><i>app.py</i><br/>Interactive Dashboard]
```
---

## Zero-Touch Automation (4-Stage CI/CD)
A core component of this MLOps architecture is the complete decoupling of pipeline stages. To ensure fault tolerance, the system uses four separate GitHub Actions workflows running on independent CRON schedules:

1. **The Ingestion Shift (`daily_ingest.yml` at 01:00):** Wakes up to securely pull the previous day's finalized weather, price, and actual CO2 data from the APIs and syncs it to Neon.
2. **The Auditor (`daily_evaluate.yml` at 02:00):** Compares the actual CO2 data from yesterday against the pipeline's predictions. It calculates error metrics and monitors data drift using Evidently AI.
3. **The Retrainer (`weekly_train.yml` at 03:00, Sundays):** Runs weekly to retrain the XGBoost models on the freshest data. It is also configured to trigger automatically if the Auditor detects an Evidently drift score exceeding `0.3`.
4. **The Forecaster (`daily_predict.yml` at 20:00):** Generates the final 70/30 scoring strategy for tomorrow's EV charging, updates the database, and pushes the new `latest_forecast.json` file directly to the frontend.

---

## Repository Structure
```text
.
├── data/                  # Raw and processed CSV datasets
├── docs/                  # Static API JSON and GitHub Pages HTML
├── models/                # Serialized XGBoost models (.pkl) for DK1 & DK2
├── notebooks/             # EDA and Model Tuning/Bake-off experiments
├── src/
│   ├── database/          # Neon SQL schema, init, and connection scripts
│   ├── frontend/          # Streamlit UI (app.py)
│   ├── pipeline/          # Core MLOps jobs (ingest, train, predict, evaluate)
│   └── utils/             # Logging and helpers
├── test/                  # Sanity checks and pre-production test scripts
├── docker-compose.yml     # Container orchestration
├── Dockerfile             # Image definition
├── requirements.txt       # Python dependencies
└── run_forecast.py        # Pipeline trigger and JSON exporter
```

## 🚀 Quickstart & Local Setup

### 1. Configuration
Create a `.env` file in the root directory and add your Neon PostgreSQL connection string. 

* **Format:** `DATABASE_URL=postgresql://[user]:[password]@[endpoint_hostname]/[dbname]?sslmode=require`
* **Example:** `DATABASE_URL=postgresql://neondb_owner:MySecretPass123@ep-cool-sun-12345.eu-central-1.aws.neon.tech/neondb?sslmode=require`

### 2. Run the Pipeline
We use Docker to ensure the environment runs perfectly on any machine.

#### Build and spin up the containers
```bash
docker-compose up -d --build
```

#### Shut containers down
```bash
docker-compose down
```

### 3. Launch the Dashboard
To view the Streamlit interface locally:
```bash
streamlit run src/frontend/app.py
```
---

---

## 🙏 Acknowledgments
Building the Greenhour Guardian end-to-end has been an incredible journey. This pipeline was developed as the final project for the MSc Data Engineering & MLOps program at Aalborg University, and I want to thank everyone who provided guidance along the way.

A massive thank you to the grading committee for taking the time to review this architecture. I also want to extend my gratitude to Aalborg University for providing access to the UCloud and AI Lab computing platforms, which were invaluable for training and testing the models. 

Finally, thank you to the open-source community, Neon Database for the brilliant serverless infrastructure, and the AI assistants (Google Gemini, Claude, and ChatGPT) that served as pair programmers and brainstorming partners throughout this build.

Kindest regards,  
**Praful Shrestha**
