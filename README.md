# GUVI_MP_harvards_artifacts_collections
A Streamlit-based ETL project integrating Harvard Art Museums API with a MySQL backend. Collect, transform, and store artifact data, then explore it using 20 predefined SQL queries and custom user queries via an interactive web interface.
Project Overview:

This project integrates Harvard Art Museum’s API with a MySQL backend and a Streamlit web interface.
It demonstrates a full ETL pipeline (Extract → Transform → Load) and allows interactive exploration of the dataset using 20 predefined SQL queries and custom user-defined queries.

Features:

ETL Pipeline: Fetch artifacts from Harvard API, transform data, and insert into MySQL.

Database Schema: Three normalized tables:
       artifact_metadata
       artifact_media
       artifact_colors

Streamlit UI:
         Collect and preview data per classification.
         Insert data into SQL.
         Run 20 predefined queries.
         Run custom SQL queries from UI.

Project Structure:

harvard_artifacts/
│── app.py              # Main Streamlit app (ETL + Queries + UI)
│── setup_database.py   # Creates database
│── create_tables.py    # Creates required tables
│── db.py               # (Optional) DB utility functions
│── README.md           # Project documentation

How Each File Works:

      setup_database.py ----> Creates database harvard_artifacts.
      create_tables.py  ----> Creates 3 tables such as
           artifact_metadata,  artifact_media,   artifact_colors

       app.py -----> Single entry point. Handles:

            Collecting Harvard API data (ETL)
            
                      Transforming & inserting into MySQL
                      Running 20 predefined queries
                      Running custom queries
                      Streamlit UI
