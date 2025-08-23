# orbitiq
Stores all documents, notebooks and resources related to the OrbitIQ MVP. 


📌 Professional Project Flow: Data Science & EO Analytics
1. Project Scoping & Planning

Define the problem statement (e.g., "Track satellites in orbit and derive spatial coverage insights").

Set success metrics (e.g., accuracy of orbit prediction, quality of visualizations).

Identify stakeholders / end-users (internal research, decision-makers, or external app users).

Deliverable planning – dashboard, model, report, or scalable API.

2. Data Acquisition & Ingestion

Catalog all data sources:

Tabular (e.g., Space-Track TLE data, NORAD catalog).

Geospatial (GeoJSON boundaries, shapefiles, raster imagery if needed).

Metadata (sensor specs, ground stations).

Set ingestion strategy: API pulls (Space-Track API), batch downloads (Celestrak JSON/CSV), or cloud storage.

Log provenance: Record source, access method, refresh frequency, licensing.

Deliverable: Data inventory document (table with source, format, size, frequency, access method).

3. Data Schema & Documentation

Column dictionary (your idea 👍): name, description, datatype, units, null rules.

Relational mapping: define how datasets connect (e.g., satellites ↔ orbits ↔ coverage zones).

Versioning: note schema evolution for reproducibility.

Deliverable: Data schema + ER diagram.

4. Data Engineering & Pipeline Setup

Cleaning & preprocessing:

Handle missing values, outliers.

Normalize units (e.g., degrees vs radians).

Time handling (UTC standardization).

Pipeline design:

Raw → Staging → Processed layers.

Automated ETL scripts (Python + Airflow/Prefect).

Storage design:

Tabular data in relational DB (Postgres/BigQuery).

Geospatial data in PostGIS/GeoParquet.

Deliverable: ETL pipeline + reproducible scripts.

5. Exploratory Data Analysis (EDA)

Statistical EDA: distributions, correlations, missingness heatmaps.

Geospatial EDA: plot orbits, ground tracks, satellite density by region.

Time-series EDA: launch trends, decay rates, coverage cycles.

Deliverable: Jupyter/Notebook report with visuals + summary insights.

6. Modeling & Analytics

Analytical models:

Orbit propagation (SGP4).

Coverage estimation (footprint calculation with Shapely/GeoPandas).

Clustering (group satellites by operator/mission).

Machine learning (if relevant):

Classification (e.g., satellite type prediction).

Regression (lifetime prediction).

Anomaly detection (unexpected orbital decay).

Deliverable: Validated models + documented assumptions.

7. EO-Specific Geospatial Integration

Spatial joins: satellites ↔ regions of interest.

Raster/vector analysis (if imagery involved):

NDVI, cloud cover, etc.

Coverage analytics:

Ground station visibility.

Satellite revisit time.

Deliverable: Geo-analytics maps + coverage heatmaps.

8. Visualization & Dashboarding

Interactive visualizations: Folium, Plotly Dash, Power BI.

Geospatial dashboards: coverage maps, time-lapse animations.

KPIs: satellites per orbit class, % coverage per region, anomaly alerts.

Deliverable: User-facing dashboard/report.

9. Validation & Quality Assurance

Cross-check results with authoritative datasets (e.g., NORAD, ESA).

Unit tests for data pipeline.

Benchmark ML models against baselines.

10. Documentation & Deployment

Technical documentation: pipeline, models, APIs.

Business logic write-up: insights, recommendations, limitations.

Deployment:

Internal: Jupyter reports + dashboards.

External: Cloud API or SaaS product.

Deliverable: End-to-end reproducible package (GitHub repo + docs).

11. Maintenance & Iteration

Monitoring: automated pipeline health checks.

Data refresh: daily/weekly ingestion jobs.

Scalability: design modular workflows for new datasets (EO imagery, GNSS).