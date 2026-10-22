# NammaMart Quick Commerce — Pipeline Assignment Submission

## Part 0: Data Profiling & Issues Log
Profiled 5,372 source records across 6 datasets. Identified and categorized issues across six dimensions:
- **Completeness**: Null customer IDs and missing delivery minutes for delivered orders.
- **Uniqueness**: Duplicate order records in daily increment files.
- **Validity**: Non-standard order statuses and irregular phone number lengths.
- **Accuracy**: Unit price, discount, and total order amount calculation mismatches.
- **Consistency**: Foreign key references to missing customers, products, and stores.
- **Timeliness**: Late-arriving payment reconciliation records.

## Part C: Scheduling Rationale
Configured Airflow DAGs with the schedule interval `0 9 * * 1-6` (9:00 AM IST, Monday through Saturday) using `pendulum.timezone("Asia/Kolkata")` with `catchup=False` to align with morning store opening hours.

## Part E & F: Governance & Observability
- **PII Protection**: Customer emails and phone numbers masked (`u***@domain.com`, `98******10`).
- **Data Quality Quarantine**: Isolated 71 failed order records into `include/quarantine/student/orders_failed_standalone.csv` without halting downstream analytical tasks.
- **Pipeline Observability**: Audit logging configured for row counts, extraction metrics, and step-level execution statuses inside DuckDB.

## Part G: CTO Architecture Recommendation
- **ELT Recommendation**: Standardize on ELT for core data warehousing to preserve full auditability of raw input data and leverage DuckDB's in-memory SQL transformation speeds.
- **ETL Exception**: Enforce strict Python-level ETL validation and PII masking at the ingestion boundary for privacy compliance and quarantine enforcement before staging.
