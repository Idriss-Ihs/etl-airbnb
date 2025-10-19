# Airbnb Data Wrangling and ETL Pipeline

### Overview
This project develops a complete **Extract–Transform–Load (ETL)** pipeline for public Airbnb data.  
The objective was to take raw, heterogeneous CSV datasets, apply systematic cleaning and integration steps, and produce an analysis-ready dataset suitable for exploratory data analysis and predictive modeling.

The workflow was built around modular, reproducible scripts using **Python (pandas, YAML, logging, Pandera)** and designed to demonstrate good data-engineering practices on large, messy, real-world data.

---

### 1. Data Sources
Data were obtained from the open **Inside Airbnb** project ([insideairbnb.com/get-the-data](http://insideairbnb.com/get-the-data)), focusing on the New York City dataset.  
The pipeline integrates the following tables:

| Dataset | Description | Approx. Size |
|----------|--------------|--------------|
| `listings.csv` | Listing metadata (price, location, room type, etc.) | 110 MB |
| `calendar.csv` | Daily availability and pricing | 1.2 GB |
| `reviews.csv` | User reviews with listing references | 400 MB |
| `neighbourhoods.csv` | Geographic boundaries and names | 1 MB |

---

### 2. ETL Design
The project follows a modular structure under `src/data/`:

| Step | Script | Purpose |
|------|---------|----------|
| **Extract** | `extract.py` | Loads or downloads raw CSVs into `data/raw/` with logging |
| **Clean** | `clean.py` | Handles missing values, normalizes types, and removes high-null columns |
| **Merge** | `merge.py` | Integrates calendar, listings, and reviews data in memory-efficient chunks |
| **Validate** | `validate.py` | Performs schema and quality checks; generates a validation report |

All parameters (paths, thresholds, filenames) are centralized in `src/config/settings.yaml` for transparency and portability.

---

### 3. Key Implementation Details
- **Logging:** Every ETL step writes to `etl_log.txt`, creating an auditable record of operations.
- **Chunked Merge:** The large `calendar.csv` file was processed in 1-million-row chunks to avoid memory overflow, allowing the pipeline to run on standard hardware.
- **Selective Loading:** Only relevant columns (`id`, `price`, `neighbourhood`, etc.) were imported to reduce memory usage and improve performance.
- **Validation:** Custom checks identify duplicate records and missing-value ratios, summarizing the results in `data/processed/validation_report.txt`.

---

### 4. Processed Output
The final dataset (`data/processed/airbnb_merged.csv`) combines availability, price, and listing metadata, enriched with aggregated review counts.  
This unified dataset is now suitable for time-based analysis, pricing models, or visualization dashboards.

---

### 5. Exploratory Analysis
A Jupyter notebook (`notebooks/01_exploration.ipynb`) was developed to explore the processed data.  
Main observations:

- Prices are right-skewed, with a concentration of listings under \$300 per night.  
- Manhattan and Brooklyn dominate the high-price range.  
- Entire homes/apartments represent the largest share of listings.  
- Availability declines in summer months, suggesting strong seasonality.  
- Listings with more reviews generally have moderate prices, indicating a balance between affordability and popularity.

Visuals were produced using **Seaborn** and **Plotly**, covering price distributions, room-type proportions, and neighborhood trends.

---

### 6. Lessons Learned
- Real-world open data demands careful **memory management** — efficient chunking was essential.
- A clean **logging and configuration structure** greatly improves maintainability.
- Automating **output stripping for notebooks** keeps repositories lightweight and compliant with GitHub limits.

---

### 7. Next Steps
- Extend the pipeline with **Airflow** or **Prefect** for orchestration.
- Integrate the ETL output into a **Power BI / Streamlit dashboard**.
- Use the processed data for a **price-prediction model** 

---

### Author
**Idris Ihs**  
_Data Scientist — ETL, Analytics, Machine Learning_  
🌐 [github.com/Idriss-Ihs](https://github.com/Idriss-Ihs)
