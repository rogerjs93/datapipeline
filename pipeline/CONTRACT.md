Data Ingestion & Normalization Pipeline - Contract

Inputs
- Files: CSV, Excel, Parquet
- SQL tables via connection string
- Mapping file (YAML/JSON) that maps source columns to canonical fields and specifies unit conversions

Outputs
- Standardized parquet files for `demographics`, `vitals`, and `labs` with canonical column names
- Validation report (JSON) listing rows failing Pydantic validation and summary statistics
- Transformer artifacts (joblib) for scaling/encoding

Success criteria
- All ingested files produce DataFrames matching Pydantic models when possible
- Unit conversions applied when source unit is provided in mapping
- Missingness policy applied per-field (see mapping defaults)

Error modes
- Missing required fields -> row-level validation error (logged) and row dropped (configurable)
- Unknown units -> flagged in validation report and left unconverted
- Corrupt file formats -> ingestion error and task failure
