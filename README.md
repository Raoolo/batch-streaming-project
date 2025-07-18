# Batch-Streaming Data Engineering Adventure

Welcome to my DE project journey! This project is about learning by doing, building a batch-streaming pipeline from scratch, and leveling up my skills step by step, literally.  
Progress is tracked visually, with "bosses" to defeat (things that got me stuck for a hot minute...) and lots of space for reflection and skill-building (what I have learned)!

---

## 🚦 Progress

`[■■□□□□□] 2/7 stages complete`

---

## 🗺️ Stages & Levels

- [x] 🏗️ **Level 1: The Forge** — Set up project repo & folder structure
    - <details><summary>Project Folder Structure</summary>
        <pre>
        BATCH-STREAMING-PROJECT/
        ├── .env
        │   └── .env
        ├── .gitignore
        ├── README.md
        ├── .github/
        │   └── workflows/
        │       └── .gitkeep
        ├── .venv/
        │   └── The usual!
        ├── dags/
        │   └── .gitkeep
        ├── dbt/
        │   └── .gitkeep
        ├── docker/
        │   ├── .dockerignore
        │   ├── .gitkeep
        │   └── Dockerfile
        └── stream_simulator/
            ├── dataset/
            │   └── synthetic_financial_data.csv
            ├── checkpoint.txt
            ├── dataset_analysis.py
            ├── requirements.txt
            └── stream_to_s3.py
        </pre>
    </details>

- [x] 🌊 **Level 2: The Stream Awakens** —  Find data & stream to S3 
    - Set up local stream
    - Set up Docker stream
    - Set up Airflow/Kubernetes
- [ ] ⏰ **Level 3: Scheduler’s Dawn** — Airflow DAG runs micro-batches
- [ ] ❄️ **Level 4: Frostbound Data** — Snowflake ingests raw data
- [ ] 🧙 **Level 5: Alchemy** — dbt transforms & cleans the data
- [ ] ⚙️ **Level 6: Automation Ritual** — CI/CD workflow (GitHub Actions) works
- [ ] 🦄 **Level 7: Vision Unlocked** — Dashboard/visualization goes live

---

## 🔄 DOING
> Big topics or for-later topics
- [ ] Implementing Airflow for Step 1 
- [ ] Write Dockerfile comments & docs

---

## 🚧 TODO
> Smaller topics

- [ ] Understand how Airflow and Kubernetes can help in this project
- [ ] Remove file formats from RAW inside snowflake STREAMING_DB\
- [ ] Solve problem regarding LIST @RAW_STAGE;

---

## ✅ TO-DONE(?)
> Things that were in the TODO or DOING section. This is a for-fun section to track my doings.

- [x] Setup directory
- [x] Setup Github
- [x] Review stream_to_s3.py code for edge cases
- [x] Implement Docker version of stream_to_s3.py
- [x] Implement tests for local deployment

---

## 🛡️ Boss Battles Won

> Things that took more than I would have expected and/or out-of-the-blue problems to solve.

- <details>
  <summary>🐍 The .venv Python mismatch (2025-06-20)</summary>

    - **Error:** Python version mismatch (32-bit vs 64-bit).
    - **Context:** My system Python was 32-bit, but the virtualenvironment was picking up a 64-bit interpreter, causingcompatibility issues.
    - **Solution:** A third party program was overriding thecorrect Python version.
  
</details>

- <details>
  <summary>🙈 File .gitignore being... Ignored? (2025-06-21)</summary>

    - **Error:** `.gitignore` file was not working properly.
    - **Context:** During pushes, I noticed some files were beingcorrectly ignored, while others not.
    - **Solution:** The file was created through Powershell,which uses an encoding that VS Code does not read properly.Just had to create the file with good old right click.
</details>

- <details>
  <summary> Docker not collaborating (2025-06-22)</summary>

    - **Error:** `CSRF session token is missing` + misalignment with folder structure
    - **Context:** Airflow setup was not pointing to correct sub-folders + .
    - **Solution:** The file was created through Powershell,which uses an encoding that VS Code does not read properly.Just had to create the file with good old right click.
</details>

- <details>
  <summary> Airflow scheduling (2025-06-22)</summary>

    - **Error:** //
    - **Context:** To get the latest S3 files uploaded with Airflow 3.0 (documentation is soo bad)
    - **Solution:** Easier than older Airflow, just a method, but the documentation was impossible to find.
</details>

- <details>
  <summary> Snowflake setup (2025-06-22)</summary>

    - **Error:** `An error occurred (AccessDenied) when calling the AssumeRole operation: User: arn:aws:iam::331317595486:user/raulo-dev is not authorized to perform: sts:AssumeRole on resource: arn:aws:iam::331317595486:role/snowflake-s3-integration-role`
    - **Context:** Shit I hated this. The error was about sts:AssumeRole but the problem was actually inside the declaration of the storage integration.
    - **Solution:** After hours, I found out I put the prefix inside the declaration of create storage integration.
</details>
---

## 📚 Skills Applied, Improved or Learned

- [x] Git basics & GitHub repo organization
- [x] AWS: S3 bucket setup, IAM users & policies
- [x] CLI and CMD/Powershell 
- [x] Python scripting for analysis and streaming data (with Polars & Boto3)
    - Virtual environment setup
    - Understood why Visual Studio is required for Python
    - `.env` for secrets, `.yaml` for configurations
    - Implemented "local" tests with pytest and moto (simulate call to S3)
- [x] Implemented CI tests through GitHub Actions Workflow
    - CI = Automate builds and tests when you push to GitHub
- [x] Dockerfile setup
  - `docker run` vs `docker compose`
- [ ] Airflow DAG creation & scheduling
- [ ] Snowflake: data loading, schema management
- [ ] dbt: modeling, SQL transformations, testing
- [ ] Docker: writing Dockerfiles, building/running containers
- [ ] CI/CD: GitHub Actions, workflow automation
- [ ] Data visualization: dashboard building (Superset or QuickSight)

---

## 📜 What’s Happening / Dev Log

> A running journal of what I’m doing, learning, and discovering as I work through each stage. This might or might not get long.

- **2025-06-20:** Set up repo, folders and virtual environment. Spent way too long solving the "bitness" mismatch (32bit vs 64bit), but now it works!
- **2025-06-22:** Stream simulation script works with Polars, as well as the Docker version. And yet it moves!
- **2025-06-23:** Airflow gave me some trouble but I beat the DAG bug. Next up: Snowflake!

---


## 🏆 Achievements Unlocked

- [x] First end-to-end data flow (simulator → S3 → Airflow)
- [ ] First successful data load to Snowflake
- [ ] All dbt tests passing
- [ ] CI/CD green on first try (speedrun!)
- [ ] Dashboard deployed

---

## 🌱 How to Use / Follow Along

1. Clone the repo, check the progress bar and levels above.
2. Follow the Dev Log or jump to Skills Learned if you’re curious what I’ve figured out so far.
3. Use the Stages as a roadmap if you want to build a similar project!

---

## 🔄 Future works
> Big topics for improvement of the pipeline
- [ ] Store the CSV data on S3 and download it at container start, or stream it directly
- [ ] Implement Kubernetes to simulate scale to production


## ☕ Motivation & Why

This README is gamified to make the process more fun and to track progress visually. No guilt, no grind—just learning, discovery, and a few (too many) boss fights.  

---

*Feel free to fork, adapt, or just follow along. Happy streaming!*

```mermaid
graph TD
    A[🏗️ The Forge<br>Project Setup] --> B[🌊 The Stream Awakens<br>Simulate/Stream to S3]
    B --> C[⏰ Scheduler’s Dawn<br>Airflow Micro-Batch]
    C --> D[❄️ Frostbound Data<br>Snowflake Ingest]
    D --> E[🧙 Alchemy<br>dbt Transforms]
    E --> F[⚙️ Automation Ritual<br>CI/CD]
    F --> G[🦄 Vision Unlocked<br>Dashboard]
```