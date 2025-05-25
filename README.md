# TMDB Pipeline
The TMDB pipeline utilizes the [TMDB API](https://developer.themoviedb.org/docs/getting-started) to extract movie data from various endpoints, transform that data, and load it into a data lake within AWS. The ingestion layer uses multiple AWS Lambda functions to fetch data from multiple endpoints. Transformations are performed using Glue Python shell jobs alongside Polars to create the business ready layer. The analytics layer also utilizes Glue and Polars to compute pre-aggregated datasets which feed into an Apache Superset Dashboard. All infrastructure within AWS is provisioned and managed using Terraform and all the tasks are orchestrated with Airflow.

## Project Structure
- `glue_utils/` – Glue helper utilities  
- `jobs/`  
  - `ingestion_jobs/` – Lambda jobs for ingesting TMDB data  
  - `transformation_jobs/` – Glue jobs for transforming ingested data  
- `src/`  
  - `analytics/` – Analytics layer logic  
  - `ingestion/` – TMDB data extraction and ingestion logic  
  - `transformation/` – Data transformation scripts  
  - `sql/` – SQL scripts
  - `utils/` – Utility functions 
- `tests/`  
  - `ingestion_tests/`  
  - `transformation_tests/`  
- `terraform/` – Infrastructure as Code (Terraform scripts for AWS)  



## Dashboard


