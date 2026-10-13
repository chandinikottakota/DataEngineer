# Healthcare & Pharma Data Platform on AWS

### Production-style Data Engineering Portfolio Project

> **Project status and accuracy note:** This repository documents the
> target architecture, implementation plan, design decisions,
> operational controls, and validation criteria for a portfolio project.
> Components should be marked **Implemented and tested** only after the
> corresponding steps have actually been run successfully. At the
> current checkpoint, the PostgreSQL source and RDS connectivity have
> been tested, and the AWS DMS source/target endpoint connection tests
> have succeeded. The DMS task has **not yet completed a successful full
> load/CDC run**: it reported a missing `s3:DeleteObject` permission on
> its S3 target role. AWS Glue transformations, Airflow orchestration,
> curated Redshift tables, Athena validation, and Power BI reporting
> must likewise be recorded as pending until individually implemented
> and verified. This status note is intentional: it makes the project
> credible and provides a clear build plan.

------------------------------------------------------------------------

## 1. Project summary

This project demonstrates how to design and build a cloud data platform
for healthcare and pharmaceutical analytics using AWS-managed services
and open-source data engineering tools.

The platform ingests relational healthcare data from PostgreSQL,
replicates the initial data and subsequent database changes to Amazon S3
using AWS Database Migration Service (AWS DMS) and Change Data Capture
(CDC), processes the data with Apache Spark through PySpark jobs on AWS
Glue, organizes datasets using a medallion architecture, validates data
quality at defined boundaries, models analytics-ready data into a star
schema, and exposes approved datasets for SQL analytics and BI reporting
through Amazon Athena and Amazon Redshift.

Apache Airflow is the workflow orchestrator. It coordinates task
dependencies, schedules processing, applies bounded retries, and
prevents downstream publication when critical upstream checks fail. AWS
IAM controls access; Amazon CloudWatch provides job logs, metrics, and
alarms. AWS Lambda is reserved for small event-driven tasks where it
adds clear value, such as handling a completion event or sending a
lightweight notification---not for replacing Spark ETL or Airflow.

### Business problem

Healthcare operations generate related data about patients, clinicians,
appointments, treatments, and billing. Reporting teams need consistent
data to answer questions such as:

-   How many appointments were scheduled, completed, or cancelled over
    time?
-   How are appointments distributed across doctors and departments,
    where the source data supports those attributes?
-   What treatment activity is associated with appointments?
-   What billing amounts are recorded by date or treatment?
-   Are source records missing, duplicated, malformed, or inconsistent?
-   How fresh is the reporting data, and did the latest pipeline run
    complete successfully?

The platform creates a repeatable ingestion-to-reporting process while
preserving raw source data, validating transformations, and publishing
controlled analytical datasets.

### Project goals

-   Ingest source data from PostgreSQL on Amazon RDS.
-   Use AWS DMS for initial full load and, after correct configuration,
    CDC replication to S3.
-   Retain a raw/bronze copy and create validated silver and
    business-ready gold layers.
-   Use AWS Glue, Apache Spark, and PySpark for scalable ETL processing.
-   Apply data-quality checks before data moves between layers and
    before Gold publication.
-   Build fact and dimension tables with documented grain and join keys.
-   Query lake data using Athena and serve warehouse-style analytics
    through Redshift.
-   Orchestrate the workflow using Airflow DAGs with dependencies,
    schedules, retries, failure handling, and safe reruns.
-   Use IAM least privilege and CloudWatch observability.
-   Provide a Power BI access path using approved Redshift reporting
    tables/views.

## 2. Architecture
flowchart TD
    SRC["PostgreSQL source database<br/>patients, doctors, appointments,<br/>treatments, billing"]
    RDS["Amazon RDS for PostgreSQL"]
    DMS["AWS DMS<br/>Full load + CDC"]
    S3B["Amazon S3 — Bronze / Raw<br/>source extracts and CDC files"]
    GLUE["AWS Glue ETL jobs<br/>Apache Spark + PySpark"]
    S3S["Amazon S3 — Silver<br/>cleaned, typed, validated data"]
    S3G["Amazon S3 — Gold<br/>facts, dimensions, aggregates"]
    ATH["Amazon Athena<br/>SQL on S3 datasets"]
    RED["Amazon Redshift<br/>analytics warehouse / serving layer"]
    BI["Power BI<br/>semantic model and dashboards"]
    AF["Apache Airflow<br/>DAGs, schedules, dependencies,<br/>retries and recovery"]
    CW["Amazon CloudWatch<br/>logs, metrics and alarms"]
    IAM["AWS IAM<br/>least-privilege roles and policies"]
    LAM["AWS Lambda (optional)<br/>small event-driven actions"]

    SRC --> RDS
    RDS --> DMS --> S3B
    S3B --> GLUE --> S3S --> GLUE --> S3G
    S3G --> ATH
    S3G --> RED --> BI
    AF -. orchestrates .-> DMS
    AF -. triggers / monitors .-> GLUE
    AF -. quality gates / load control .-> RED
    LAM -. optional events / notifications .-> AF
    DMS -. logs / metrics .-> CW
    GLUE -. logs / metrics .-> CW
    AF -. task logs / alerts .-> CW
    IAM -. permissions .-> RDS
    IAM -. permissions .-> DMS
    IAM -. permissions .-> S3B
    IAM -. permissions .-> GLUE
    IAM -. permissions .-> RED
```

### End-to-end data flow

1.  Source: PostgreSQL stores the operational healthcare entities
    and their relationships.
2.  Database hosting: Amazon RDS for PostgreSQL hosts the source
    database for this learning environment.
3.  Ingestion: AWS DMS performs an initial full load. Once CDC is
    configured and validated, DMS captures supported ongoing changes and
    writes them to the S3 target.
4.  Bronze / Raw: S3 retains the landed source data and CDC output.
    Raw data is treated as an ingestion record, not as the trusted
    reporting layer.
5.  Silver: AWS Glue runs PySpark on Apache Spark to parse data,
    standardize types, clean values, deduplicate according to documented
    keys, apply validation rules, and quarantine rejected records.
6.  Gold: Glue transformations produce analytics-ready fact tables,
    dimension tables, and any justified aggregates.
7.  Lake queries: Athena queries supported S3 datasets for ad hoc
    SQL analysis, validation, and exploration.
8.  Warehouse serving: Curated Gold datasets are loaded into Amazon
    Redshift for structured analytical querying and BI consumption.
9.  Reporting: Power BI connects to approved Redshift reporting
    tables/views, builds a semantic model, and presents dashboards.
10. Orchestration and operations: Airflow controls task order and
    readiness, while CloudWatch captures logs and metrics. IAM grants
    each service only the access it needs.

**Important distinction:** DMS is responsible for database replication
and CDC capture; it is not the transformation engine. Spark performs the
transformations. Airflow coordinates workflow tasks; it does not replace
DMS or execute the distributed Spark computation itself.

## 3. Technology stack and responsibilities

  -----------------------------------------------------------------------
  Technology                          Responsibility in this project
  ----------------------------------- -----------------------------------
  PostgreSQL                          Relational source database and
                                      SQL-based source validation

  Amazon RDS for PostgreSQL           Managed hosting for the source
                                      database

  AWS DMS                             Initial full load and configured
                                      ongoing CDC replication

  Amazon S3                           Durable storage for Bronze, Silver,
                                      Gold, quarantine, and run-control
                                      data

  Apache Spark                        Distributed processing engine used
                                      by Glue ETL jobs

  PySpark                             Python API used to express Spark
                                      DataFrame transformations

  AWS Glue                            Managed Spark ETL runtime, job
                                      configuration, and integration with
                                      the AWS data lake

  AWS Glue Data Catalog               Metadata/catalog definitions used
                                      by compatible query and processing
                                      services

  Amazon Athena                       Serverless SQL queries on cataloged
                                      data in S3

  Amazon Redshift                     Analytical warehouse and controlled
                                      BI-serving layer

  Apache Airflow                      DAG orchestration, task
                                      dependencies, schedules, retries,
                                      and workflow status

  AWS Lambda                          Optional lightweight event handling
                                      or notification; not the main ETL
                                      engine

  AWS IAM                             Authentication and authorization
                                      through roles and least-privilege
                                      policies

  Amazon CloudWatch                   Logs, metrics, alarms, and
                                      operational troubleshooting

  Power BI                            Semantic model, measures,
                                      dashboards, and reporting
  -----------------------------------------------------------------------

### Spark, PySpark, and Glue: how they fit together

-   **Apache Spark** is the distributed processing engine.
-   **PySpark** is the Python interface used to write Spark code.
-   **AWS Glue** is the managed AWS service/runtime that runs our
    PySpark ETL jobs.

For example, a PySpark job reads raw appointment files from S3, casts
date columns, checks required fields, joins appointment records to
patient and doctor data, and writes validated Parquet files to Silver.
Glue provisions and runs the job; Spark plans and executes the
transformations.

The dataset used for learning is small, so the project does not claim
that Spark is necessary for this dataset's volume. Spark is included to
learn the distributed-processing model and a common cloud ETL pattern.

## 4. Source system and data model

The source database contains the following entities. The actual schema,
columns, and constraints in the PostgreSQL database are the source of
truth; update this section if the schema changes.

  -----------------------------------------------------------------------
  Table                   Purpose                 Relationship / expected
                                                  key
  ----------------------- ----------------------- -----------------------
  `patients`              Patient                 `patient_id`
                          reference/demographic   
                          attributes permitted    
                          for the project         

  `doctors`               Clinician reference     `doctor_id`
                          attributes              

  `appointments`          Appointment events or   `appointment_id`;
                          bookings                references patient and
                                                  doctor

  `treatments`            Treatment records       `treatment_id`;
                          associated with an      references appointment
                          appointment             

  `billing`               Billing records         `billing_id`;
                          associated with a       references treatment
                          treatment               
  -----------------------------------------------------------------------

Expected referential relationships:

-   `appointments.patient_id` → `patients.patient_id`
-   `appointments.doctor_id` → `doctors.doctor_id`
-   `treatments.appointment_id` → `appointments.appointment_id`
-   `billing.treatment_id` → `treatments.treatment_id`

These relationships were checked against the sample data during initial
dataset validation. Re-run the checks against the current RDS database
and every new ingestion batch; earlier successful checks do not
guarantee future data quality.

### Source validation before ingestion

Before enabling a pipeline run, validate:

-   Source database and required tables are reachable.
-   Required tables and columns exist.
-   Primary keys are populated and unique.
-   Foreign-key relationships are valid.
-   Column types and date formats match the expected schema.
-   Source row counts are recorded for reconciliation.
-   The DMS endpoint test and replication task status are successful.
-   CDC prerequisites and permissions are configured for the actual
    PostgreSQL/RDS setup.

Do not assume that a successful endpoint connection test means a full
load or CDC task has completed successfully.

## 5. PostgreSQL, RDS, DMS, and CDC ingestion

### PostgreSQL on RDS

Amazon RDS hosts the PostgreSQL source. Source tables are created with
primary keys and foreign-key constraints where appropriate. Use a
dedicated database user for replication and administration, rather than
sharing an unrestricted user across services.

Keep the RDS instance private wherever feasible. Restrict security-group
ingress to the required source clients and ports. Do not place database
passwords in code or commit them to the repository.

### AWS DMS: full load and CDC

AWS DMS is configured with:

-   A source endpoint for the PostgreSQL database.
-   A target endpoint for Amazon S3.
-   A replication instance.
-   A replication task selecting the intended tables and configuring
    full-load and CDC behavior.
-   A target S3 bucket/prefix and an IAM role with the necessary
    permissions.

**Full load** copies the existing rows at the time of the initial
migration. **CDC** captures subsequent supported database changes so the
target can be updated over time. The precise DMS target file layout and
operation metadata depend on task and endpoint settings; inspect the
actual output before writing downstream parsing logic.

DMS CDC output in S3 is not automatically equivalent to a final,
deduplicated table. The processing layer must understand insert, update,
and delete events, operation ordering, keys, and any available
timestamps or sequence metadata. If multiple events affect one key,
apply the events using a defined ordering rule and a deterministic
latest-state strategy. Do not simply append every change and assume the
result is the current state.

### DMS target layout

An example layout---not a guarantee of DMS's exact generated paths---is:

``` text
s3://healthcare-data-platform-de/
└── raw/
    └── public/
        ├── patients/
        ├── doctors/
        ├── appointments/
        ├── treatments/
        └── billing/
```

Confirm the real prefixes from the objects DMS writes. Do not create
empty folders and treat them as proof of ingestion.

### DMS setup and failure handling

A DMS task can require permissions to list, write, and sometimes delete
objects depending on its target configuration and preparation mode.
During this project's initial attempt, DMS logged that the role
`DMSS3AccessRole-healthcare-data-platform-de-raw` lacked
`s3:DeleteObject` access for objects under the raw prefix while using a
`DROP_AND_CREATE` preparation mode.

Remediation checklist:

1.  Identify the exact IAM role attached to the DMS S3 target endpoint.
2.  Review the task's target preparation mode. `DROP_AND_CREATE` may
    require deletion privileges; do not grant delete access
    automatically if a safer preparation mode meets the requirement.
3.  Scope any required S3 permissions to the specific project bucket and
    relevant prefix.
4.  Check bucket policy, role trust policy, encryption/KMS permissions
    if applicable, and endpoint settings.
5.  Re-test endpoints, then restart the task using the correct recovery
    choice.
6.  Verify the task state, table statistics, logs, S3 object contents,
    and row counts.
7.  Verify CDC separately by making a controlled test
    insert/update/delete and checking the resulting target
    representation.

Never state that CDC works solely because endpoint tests passed. Record
evidence of a completed initial load and a verified post-load change
test.

## 6. S3 medallion architecture

The data lake is organized into three logical data-quality layers. These
are logical layers and can be implemented as prefixes in one bucket or
separated into buckets if access-control and lifecycle requirements
justify it.

### Bronze --- raw ingestion

**Purpose:** Preserve source extracts and DMS CDC output as received,
with minimal modification.

-   Store the original landed files.
-   Preserve table identity, ingestion/run metadata, and DMS operation
    metadata when available.
-   Avoid silently overwriting historical raw data.
-   Restrict access because raw healthcare data may contain sensitive
    fields.
-   Record object arrival, file sizes, and expected table/prefix
    readiness.
-   Do not expose Bronze directly to ordinary BI users.

Example prefix:

``` text
s3://healthcare-data-platform-de/bronze/
```

If DMS lands directly in `raw/`, either treat that prefix as the Bronze
layer in the documentation or configure an explicit copy/organization
step. Avoid maintaining two names for the same physical layer without
explaining the relationship.

### Silver --- clean and validated

**Purpose:** Provide standardized, typed, deduplicated, quality-checked
data.

Typical processing:

-   Parse source data and apply an explicit schema.
-   Trim strings and normalize formats where appropriate.
-   Convert dates, timestamps, and numeric values to suitable types.
-   Validate required identifiers and supported business rules.
-   Resolve duplicates using documented keys and deterministic ordering.
-   Validate foreign-key references.
-   Interpret CDC operations and create the intended current-state or
    history representation.
-   Route invalid records to quarantine with a reason and run
    identifier.
-   Write validated datasets in Parquet where suitable.

Example prefixes:

``` text
s3://healthcare-data-platform-de/silver/patients/
s3://healthcare-data-platform-de/silver/doctors/
s3://healthcare-data-platform-de/silver/appointments/
s3://healthcare-data-platform-de/silver/treatments/
s3://healthcare-data-platform-de/silver/billing/
```

### Gold --- analytics-ready data

**Purpose:** Publish stable, business-oriented data models for SQL
analytics and reporting.

Typical outputs:

``` text
s3://healthcare-data-platform-de/gold/dimensions/
s3://healthcare-data-platform-de/gold/facts/
s3://healthcare-data-platform-de/gold/aggregates/
```

Gold contains documented dimensions, fact tables, and optional aggregate
datasets. The layer is published only after required upstream jobs and
quality gates pass. Gold should not be a copy of Silver with a new
folder name; it should have a clear analytical model and business
meaning.

### Quarantine and control metadata

``` text
s3://healthcare-data-platform-de/quarantine/
    invalid_records/
    quality_failures/

s3://healthcare-data-platform-de/control/
    manifests/
    run_metadata/
    checkpoints/
```

Quarantine stores rejected records and failure reasons subject to
appropriate access restrictions and retention rules. Control metadata
can store run IDs, input manifests, watermarks, row counts, quality
results, and publication status. Never store secrets in these locations.

## 7. AWS Glue, Apache Spark, and PySpark ETL design

Create separate, understandable jobs rather than one oversized script. A
possible job design is:

1.  `glue_bronze_to_silver_reference`: clean and validate patients and
    doctors.
2.  `glue_bronze_to_silver_appointments`: clean appointment data and
    validate patient/doctor references.
3.  `glue_bronze_to_silver_treatments`: validate appointment references
    and treatment fields.
4.  `glue_bronze_to_silver_billing`: validate treatment references and
    billing fields.
5.  `glue_silver_to_gold_model`: build fact/dimension datasets and
    required aggregates.
6.  `glue_publish_gold_to_redshift`: load or merge curated datasets into
    Redshift using a controlled load strategy.

These are proposed job boundaries. Consolidate or split them based on
actual dependency, execution time, and operational needs.

### Example transformation pattern

The following is a simplified illustration of a Silver transformation.
Replace the example bucket and verify actual source column names and
formats before running it.

``` python
from pyspark.sql import functions as F

source_path = "s3://YOUR_BUCKET/bronze/appointments/"
target_path = "s3://YOUR_BUCKET/silver/appointments/"

appointments = (
    spark.read
    .option("header", "true")
    .csv(source_path)
)

# Inspect the source before applying production rules.
appointments.printSchema()

cleaned = (
    appointments
    .withColumn("appointment_id", F.trim(F.col("appointment_id")))
    .withColumn("patient_id", F.trim(F.col("patient_id")))
    .withColumn("doctor_id", F.trim(F.col("doctor_id")))
    .withColumn("appointment_date", F.to_date(F.col("appointment_date")))
)

# Example critical-field rule. Real jobs should count and quarantine failures.
valid = cleaned.filter(
    F.col("appointment_id").isNotNull()
    & (F.col("appointment_id") != "")
    & F.col("patient_id").isNotNull()
    & F.col("doctor_id").isNotNull()
)

# A duplicate strategy must be deterministic when duplicate source rows
# contain different versions. This simplified example is only for learning.
valid = valid.dropDuplicates(["appointment_id"])

(
    valid.write
    .mode("overwrite")
    .format("parquet")
    .save(target_path)
)
```

This example demonstrates the API, not a complete production CDC
implementation. A production job needs an explicit schema,
input-manifest/run handling, deterministic duplicate resolution,
quarantine output, quality metrics, safe writes, and an incremental CDC
strategy. `overwrite` should only be used for an intentional full
refresh or an isolated test; it is not a general solution for
incremental production data.

### Recommended job parameters

Use configurable parameters rather than hard-coding environment-specific
values:

-   `--environment` (dev/test/prod)
-   `--source_prefix`
-   `--target_prefix`
-   `--run_id`
-   `--processing_date` or controlled watermark
-   `--load_type` (full/incremental)
-   `--table_name`
-   `--quality_mode` (fail/quarantine based on rule severity)

Use a managed Glue execution role. Store credentials in a suitable
secret-management service if credentials are needed; do not embed
credentials in scripts.

## 8. Data-quality framework: what is checked and where

Data quality is checked at several boundaries---not just once at the
end. Every check should record the rule, dataset, run ID, observed
value, threshold, pass/fail result, and failure reason.

  -----------------------------------------------------------------------
  Pipeline boundary       Checks                  Failure response
  ----------------------- ----------------------- -----------------------
  PostgreSQL source       Connectivity, required  Stop ingestion or flag
                          tables/columns, schema, source readiness
                          primary keys, foreign   failure
                          keys, source counts     

  DMS → Bronze            Task status, expected   Do not launch Silver
                          tables/prefixes, file   until input readiness
                          arrival, object sizes,  is established
                          load status, CDC task   
                          errors                  

  Bronze input validation Schema drift, parse     Quarantine bad records;
                          failures, malformed     fail if a critical
                          rows, missing           threshold is exceeded
                          identifiers,            
                          duplicate/event checks  

  Silver validation       Required fields, types, Block dependent Gold
                          unique keys, valid      jobs when critical
                          dates/ranges, allowed   rules fail
                          statuses where defined, 
                          referential integrity   

  CDC processing          Operation type, key     Fail the incremental
                          presence, event         run and preserve
                          ordering, delete        recovery state
                          handling, duplicate     
                          events,                 
                          watermark/checkpoint    
                          consistency             

  Gold model              Fact grain,             Do not publish invalid
                          dimension-key coverage, Gold or load it to
                          join cardinality,       Redshift
                          aggregate               
                          reconciliation,         
                          duplicate fact keys     

  Redshift publication    Loaded row counts, key  Mark publication failed
                          checks, expected        and keep the previous
                          totals,                 valid reporting
                          transaction/load        snapshot available
                          status, freshness       where the load design
                                                  supports it

  Reporting freshness     Latest successful run,  Alert support/owner;
                          source-to-report lag,   display or record
                          refresh status          freshness status
  -----------------------------------------------------------------------

### Example quality rules

**Completeness** - All required source tables have arrived. - Required
business keys are not null. - Critical fields meet an agreed
completeness threshold.

**Uniqueness** - `patient_id` is unique in the current patient
dimension. - `doctor_id` is unique in the current doctor dimension. -
`appointment_id` is unique in the current appointment entity if the
source model defines one row per appointment. - Fact uniqueness is
tested at the declared grain, not by an arbitrary set of columns.

**Referential integrity** - Every appointment patient key matches a
valid patient. - Every appointment doctor key matches a valid doctor. -
Every treatment appointment key matches an appointment. - Every billing
treatment key matches a treatment.

**Validity** - Dates parse correctly and follow agreed range rules. -
Numeric amounts are numeric and conform to the actual source/business
rules. - Status fields use recognized values where the source has a
controlled list.

**Reconciliation** - Compare input, accepted, quarantined, and output
row counts. - Compare relevant billing totals before and after
transformations using the same filters and business definition. -
Investigate expected changes caused by deduplication, CDC deletes,
rejected records, and join cardinality.

### Quality-gate behavior

Not every rule should necessarily stop the entire platform. Classify
rules:

-   **Critical:** missing source/table, broken schema contract,
    unavailable required input, invalid key relationships that would
    corrupt facts, unresolved CDC ordering, or failed publication. Stop
    downstream processing.
-   **Warning:** a non-critical threshold breach with a documented owner
    and acceptable policy. Alert and continue only when the rule's
    approved policy explicitly permits it.
-   **Record-level rejection:** send bad rows to quarantine with a
    reason, while valid rows continue only if the dataset policy allows
    partial acceptance.

Never silently drop records. Store counts for received, accepted,
rejected, and published records. For healthcare data, restrict access to
quarantined content as carefully as raw data.

## 9. Dimensional modeling and star schema

The Gold layer is designed for analytics. A star schema has fact tables
containing measurable business events and foreign keys to descriptive
dimension tables.

### Dimension tables

  --------------------------------------------------------------------------
  Dimension                  Purpose                 Key design
  -------------------------- ----------------------- -----------------------
  `dim_patient`              Approved patient        Surrogate
                             attributes for          `patient_key`; retain
                             reporting               source `patient_id` as
                                                     a business key

  `dim_doctor`               Doctor attributes       Surrogate `doctor_key`;
                             available in the source retain source
                                                     `doctor_id`

  `dim_date`                 Calendar attributes     Surrogate or date-based
                             such as date, month,    key, consistently
                             quarter, and year       defined

  `dim_appointment_status`   Standardized            Status key and
  (optional)                 appointment-status      controlled status
                             descriptions, if        attributes
                             supported by the source 
  --------------------------------------------------------------------------

Use only fields actually available and approved in the source. Avoid
putting unnecessary patient-identifying or sensitive attributes in the
BI layer. If patient attributes change over time, decide whether the
project uses a Type 1 overwrite or a Type 2 history dimension; document
and implement one approach rather than claiming both.

### Fact tables and grain

**Fact table grain** is the precise definition of what one row
represents. Define it before joining tables or adding measures.

  -----------------------------------------------------------------------
  Fact                    Proposed grain          Example measures
  ----------------------- ----------------------- -----------------------
  `fact_appointment`      One row per source      Appointment count
                          appointment             (usually 1 per row);
                                                  other measures only if
                                                  source supports them

  `fact_treatment`        One row per source      Treatment count; other
                          treatment               numeric measures only
                                                  if present and
                                                  meaningful

  `fact_billing`          One row per source      Billed amount and other
                          billing record          source-provided
                                                  financial measures
  -----------------------------------------------------------------------

Each fact includes the relevant dimension keys and useful source
business identifiers for traceability. A fact may include a degenerate
business identifier where appropriate, but it must not duplicate
sensitive details unnecessarily.

### Avoid join fan-out and inflated measures

The source has one-to-many relationships: an appointment can have
multiple treatments, and a treatment can have multiple billing records
depending on the source model. Joining all five source tables into one
flat dataset can multiply rows and inflate appointment counts or billing
totals.

Instead:

1.  Validate each source table and relationship.
2.  Build each fact at its declared grain.
3.  Join dimensions using the appropriate key mapping.
4.  Aggregate measures only at the correct grain.
5.  Reconcile row counts and totals against the source using equivalent
    filters.

For example, calculate billing measures from the billing grain, not from
a joined appointments--treatments--billing dataset unless the
multiplicity is explicitly accounted for.

### Star schema illustration

``` mermaid
flowchart TD
    DP["dim_patient"]
    DD["dim_doctor"]
    DT["dim_date"]
    FS["fact_appointment"]
    DSTAT["dim_appointment_status"]
    FT["fact_treatment"]
    FB["fact_billing"]

    DP --> FS
    DD --> FS
    DT --> FS
    DSTAT --> FS
    DT --> FT
    DT --> FB
```

This is a conceptual illustration, not a requirement to force every fact
into one shared star. In implementation, each fact should have the
dimensions appropriate to its grain. Treatment and billing facts may
need appointment or treatment dimensions/keys, depending on the final
model. Document each relationship and test the cardinality.

## 10. Athena and Redshift: why use both?

They solve related but different analytical needs.

### Amazon Athena

Athena runs SQL queries against data stored in S3, using catalog
metadata for supported formats.

Use Athena for:

-   Inspecting Bronze/Silver/Gold files without loading them into a
    warehouse.
-   Checking sample records and schema.
-   Validating row counts and aggregates after Glue jobs.
-   Investigating data-quality failures.
-   Ad hoc exploration and data-lake analytics.

Athena does not transform the lake by itself; transformation is
performed by Glue/Spark or other processing jobs. Query cost and
performance depend on scanned data, file format, partitioning, and query
design.

### Amazon Redshift

Redshift is the analytical warehouse/serving layer.

Use Redshift for:

-   Loading curated Gold facts and dimensions.
-   Repeated analytical queries and BI workloads.
-   Providing a controlled schema of reporting tables and views.
-   Applying warehouse-oriented access controls and workload management
    as appropriate.
-   Serving Power BI dashboards from a stable analytical model.

Both Athena and Redshift are included to demonstrate complementary
patterns. They are not always both necessary in every real system. In
this project, Athena is the lake-validation and exploration path;
Redshift is the primary BI-serving path.

### Gold-to-Redshift load

The publication step should:

1.  Confirm all required Gold outputs and quality gates passed.
2.  Use a documented load method supported by the chosen Redshift
    configuration.
3.  Load into staging tables or a controlled intermediate area.
4.  Validate keys, row counts, and relevant totals.
5.  Merge/upsert or replace the target data using a deliberate full or
    incremental strategy.
6.  Publish only after validation succeeds.
7.  Record the run ID, source watermark, loaded counts, and final
    status.

Do not expose a partially loaded set of tables as a successful reporting
refresh.

## 11. Power BI reporting flow

The primary reporting path is:

``` text
PostgreSQL / RDS
      ↓
DMS full load + CDC
      ↓
S3 Bronze → Glue/PySpark → Silver → Gold
      ↓
Redshift reporting schema (facts, dimensions, approved views)
      ↓
Power BI connector
      ↓
Power BI semantic model, measures, dashboards
```

Implementation approach:

1.  Create the curated tables and approved views in Redshift.
2.  Create a dedicated read-only reporting identity/role with access
    only to the required schemas, tables, and views.
3.  Connect Power BI Desktop using the supported Amazon Redshift
    connector and approved connection details.
4.  Select the intended tables/views and build a semantic model with
    appropriate relationships, measures, and date handling.
5.  Build dashboards for appointment activity, treatment activity,
    billing totals, and data freshness, subject to fields actually
    present in the dataset.
6.  Publish to Power BI Service if available in the learning
    environment.
7.  Configure credentials, network connectivity, and scheduled refresh.
    Depending on the Redshift network and Power BI deployment, an
    approved gateway or other network configuration may be required.
8.  Verify that dashboard totals match validated Redshift queries and
    that users cannot access unauthorized raw or patient-sensitive data.

Power BI is not part of the ingestion engine. It is the reporting layer
on top of curated analytical data.

## 12. Apache Airflow orchestration

Airflow organizes the workflow as DAGs---Directed Acyclic Graphs. A DAG
defines tasks and their dependencies, but the tasks do the actual work
through DMS controls, Glue jobs, SQL checks, and publication steps.

### Example DAG dependency

``` mermaid
flowchart TD
    A["check_source_and_dms_status"] --> B["wait_for_input_readiness"]
    B --> C["validate_bronze_inputs"]
    C --> D["glue_patients_doctors_to_silver"]
    C --> E["glue_appointments_to_silver"]
    C --> F["glue_treatments_to_silver"]
    C --> G["glue_billing_to_silver"]
    D --> H["silver_quality_checks"]
    E --> H
    F --> H
    G --> H
    H --> I["build_gold_dimensions_and_facts"]
    I --> J["gold_quality_and_reconciliation"]
    J --> K["load_redshift_staging"]
    K --> L["validate_redshift_load"]
    L --> M["publish_reporting_tables"]
    M --> N["record_run_and_notify"]
```

This DAG is a proposed workflow. Exact task operators and trigger rules
depend on the Airflow deployment and how the AWS services are invoked.

### Scheduling

Choose a schedule that matches the data freshness objective and the
ingestion behavior. For a learning environment, a daily schedule is a
reasonable starting point, but the schedule is not a claim that CDC
itself only runs daily. DMS may continuously capture changes while
Airflow periodically processes the landed changes.

A production schedule should specify:

-   Time zone and expected run window.
-   Whether overlapping runs are allowed.
-   How the pipeline detects that new input is ready.
-   The expected freshness SLA.
-   Maximum active runs and concurrency.
-   How backfills are performed.
-   How a late or missing input is handled.

Do not start a downstream job merely because the clock says it is time.
Confirm input readiness and CDC processing boundaries.

### Retries and failure handling

Configure bounded retries for transient failures such as throttling,
short-lived network interruptions, or temporary service unavailability.
Use a delay/backoff appropriate to the error.

Do not repeatedly retry deterministic errors such as a missing IAM
permission, an incompatible schema, or invalid code without first
correcting the cause. These errors require diagnosis and remediation,
not an unlimited retry loop.

Recommended behavior:

-   **Source/DMS not ready:** wait or fail the run; do not process
    incomplete inputs.
-   **Glue transient failure:** retry within a defined limit; inspect
    logs if it continues.
-   **Quality gate failure:** fail dependent Gold/Redshift tasks and
    retain failure details.
-   **Redshift load failure:** do not mark the run published; safely
    retry from staging when possible.
-   **Notification failure:** record the notification error without
    misrepresenting the data pipeline result.
-   **Repeated failure:** alert the owner and create a clear runbook
    action.

### Idempotency and safe reruns

A rerun should not duplicate records or corrupt the latest reporting
data. Design jobs so that:

-   Each run has a unique `run_id`.
-   Inputs and processing boundaries are recorded.
-   CDC events are applied using stable keys and deterministic ordering.
-   Checkpoints/watermarks are advanced only after the associated output
    is committed and validated.
-   Output writes use safe staging-and-publish patterns or
    transactional/merge mechanisms where supported.
-   Retrying a completed batch produces the same intended target state.
-   A failed run can resume from a known boundary without silently
    skipping data.

## 13. AWS Lambda: intended use

Lambda is an optional supporting component, not a mandatory
transformation layer. Include it only for a small, clearly defined
event-driven task, such as:

-   Handling a small completion event or creating a lightweight run
    notification.
-   Validating an incoming manifest and notifying the orchestration
    layer.
-   Sending a notification after a successful or failed pipeline state.

Avoid using Lambda to process large datasets, replacing Glue/Spark, or
duplicating Airflow's retry and dependency logic. If no real need
exists, leave Lambda as a documented extension rather than adding an
unnecessary service.

## 14. IAM, secrets, and data security

Use least privilege and separate service roles where practical.

  -----------------------------------------------------------------------
  Identity / role                     Required access pattern
  ----------------------------------- -----------------------------------
  DMS target role                     Only the required S3 target-prefix
                                      permissions and encryption access,
                                      including deletion only if the
                                      configured task mode requires it

  Glue execution role                 Read required Bronze prefixes;
                                      write
                                      Silver/Gold/quarantine/control
                                      outputs; access catalog/logging and
                                      other explicitly required services

  Airflow execution identity          Trigger/monitor only the required
                                      jobs and access orchestration
                                      metadata/secrets

  Redshift load identity              Write to staging and target schemas
                                      needed by the pipeline

  BI reporting identity               Read only approved reporting
                                      schemas/views

  Developer/admin identity            Administrative permissions only
                                      when needed, with credentials
                                      protected
  -----------------------------------------------------------------------

Security practices:

-   Do not commit access keys, passwords, tokens, or connection strings
    containing secrets.
-   Restrict RDS network access and S3 access.
-   Use encryption in transit and at rest where supported and
    configured.
-   Protect KMS keys and secrets with least privilege.
-   Avoid exposing patient identifiers or sensitive raw fields to broad
    BI audiences.
-   Apply retention and deletion policies appropriate to the portfolio
    environment.
-   Use non-production sample data; do not use real patient records.
-   Review CloudWatch log content to avoid unnecessarily logging
    sensitive row values.

## 15. Monitoring, logging, and operational runbook

For each run, record:

-   Run ID, start/end timestamps, environment, and processing window.
-   Source/DMS task state and input manifest.
-   Input, accepted, rejected, and output record counts.
-   Data-quality rule results and reconciliation outcomes.
-   Glue job run IDs and final states.
-   Gold publication and Redshift load status.
-   Latest successful processing watermark and data freshness.
-   Error category, retry count, and final failure reason.

### Troubleshooting examples

  --------------------------------------------------------------------------
  Symptom                 Likely investigation    Safe response
  ----------------------- ----------------------- --------------------------
  DMS task fails on S3    Inspect target role,    Correct the narrowly
  access                  bucket policy, prefix,  scoped
                          task preparation mode,  permission/configuration
                          and encryption          and retest
                          permissions             

  DMS task starts but     Inspect task/table      Resolve the table-specific
  tables do not load      status, source          issue; confirm loaded
                          permissions, table      counts
                          mappings, logs, and     
                          target prefixes         

  New raw objects are     Inspect DMS status,     Hold Silver processing
  missing                 target prefix,          until inputs are complete
                          permissions, and        
                          expected input boundary 

  Glue job fails to read  Inspect execution role, Fix access/path/schema and
  S3                      bucket/KMS policy,      rerun safely
                          path, file format, and  
                          logs                    

  Schema changes          Compare actual schema   Quarantine or stop on
  unexpectedly            with the contract and   critical drift; approve
                          inspect parse errors    and version schema changes

  Counts increase         Inspect duplicate keys, Stop publication and
  unexpectedly            CDC replay, join        reconcile before rerunning
                          cardinality, and        
                          incremental boundaries  

  Billing totals are too  Check one-to-many joins Rebuild from the correct
  high                    and the declared fact   billing grain and
                          grain                   reconcile totals

  Glue job is slow        Inspect Spark           Tune only after
                          UI/execution plan,      identifying the bottleneck
                          shuffle, partition      
                          sizes, skew, and file   
                          sizes                   

  Airflow task retries    Inspect error class and Retry transient errors;
  repeatedly              service logs            fix deterministic
                                                  permission/schema/code
                                                  errors

  Power BI numbers differ Compare filters,        Validate the same
  from Redshift           relationships,          definition and data
                          semantic-model          snapshot end to end
                          measures, and refresh   
                          timestamp               
  --------------------------------------------------------------------------

### CloudWatch

Configure or inspect service logs and metrics for DMS, Glue, Lambda (if
used), and the Airflow environment as applicable. Add alarms for
meaningful conditions, such as repeated job failures or unacceptable
data freshness. Exact metric names and available alarms depend on
service configuration and the chosen Airflow deployment.

## 16. Incremental processing and CDC correctness

CDC is more than "read the latest files." A reliable incremental process
needs a documented event-handling contract.

1.  Identify how DMS represents inserts, updates, and deletes in the
    selected S3 target configuration.
2.  Preserve source business keys and operation metadata needed to
    interpret changes.
3.  Establish deterministic ordering when several events affect the same
    key. Use the metadata actually available from DMS; do not invent an
    ordering column.
4.  Define how deletes are represented in Silver and how they affect
    facts/dimensions.
5.  Record the processed input boundary or manifest.
6.  Apply the batch to staging or another recoverable intermediate
    output.
7.  Validate counts, key integrity, and the resulting target state.
8.  Commit/publish the output and only then advance the
    checkpoint/watermark.
9.  Test replay and restart behavior with controlled insert, update, and
    delete scenarios.

The exact implementation depends on the chosen DMS output format and CDC
settings. Inspect actual output first, then build the parser and merge
strategy. A raw append-only collection of CDC files is useful history
but is not, by itself, a current-state table.

## 17. Suggested repository structure

``` text
healthcare-data-platform/
├── README.md
├── docs/
│   ├── architecture.md
│   ├── source-data-dictionary.md
│   ├── data-quality-rules.md
│   ├── star-schema.md
│   ├── operations-runbook.md
│   └── decisions-and-tradeoffs.md
├── sql/
│   ├── source_validation.sql
│   ├── silver_validation.sql
│   ├── redshift_ddl.sql
│   └── reconciliation.sql
├── glue_jobs/
│   ├── bronze_to_silver_patients.py
│   ├── bronze_to_silver_doctors.py
│   ├── bronze_to_silver_appointments.py
│   ├── bronze_to_silver_treatments.py
│   ├── bronze_to_silver_billing.py
│   └── silver_to_gold.py
├── airflow/
│   └── dags/
│       └── healthcare_data_pipeline.py
├── lambda/
│   └── notify_pipeline_status.py
├── tests/
│   ├── test_data_quality.py
│   ├── test_cdc_logic.py
│   └── test_reconciliation.py
└── screenshots/
    └── README.md
```

Only include files that actually exist in the repository. A suggested
folder structure is not proof that the components are implemented.

## 18. Implementation and verification checklist

Use this checklist to track actual progress. Mark an item complete only
after running it and retaining evidence such as logs, SQL output,
screenshots, or tests.

### Phase A --- Source and ingestion

-   [x] PostgreSQL/RDS connection established in pgAdmin.
-   [x] Source tables created with the intended relationships.
-   [x] DMS source and target endpoint connection tests succeeded.
-   [ ] Review the DMS S3 target role and correct the missing
    `s3:DeleteObject` permission or choose an appropriate safer
    preparation mode.
-   [ ] Run DMS full load successfully for all selected tables.
-   [ ] Verify S3 output and loaded table statistics.
-   [ ] Run controlled insert/update/delete tests and verify CDC output.
-   [ ] Document the actual DMS file format, path layout, and event
    metadata.

### Phase B --- Lake and Spark processing

-   [ ] Confirm and document Bronze prefixes and ingestion-readiness
    logic.
-   [ ] Create Glue execution role with least-privilege permissions.
-   [ ] Create and run a PySpark Glue job against a real Bronze dataset.
-   [ ] Produce Silver Parquet datasets.
-   [ ] Implement quarantine output and quality metrics.
-   [ ] Test schema drift, null keys, duplicates, invalid dates, and
    broken references.
-   [ ] Implement and test CDC merge/delete behavior and safe reruns.
-   [ ] Build Gold dimensions and facts at documented grains.

### Phase C --- Query and serving

-   [ ] Register appropriate datasets in the Glue Data Catalog.
-   [ ] Query Silver/Gold datasets with Athena and save validation SQL.
-   [ ] Create Redshift staging and target tables.
-   [ ] Load Gold outputs to Redshift using a controlled, rerunnable
    method.
-   [ ] Validate Redshift row counts, keys, totals, and freshness.
-   [ ] Connect Power BI to approved Redshift reporting objects if the
    environment supports it.

### Phase D --- Orchestration and operations

-   [ ] Create Airflow DAG tasks and dependencies.
-   [ ] Configure a schedule and input-readiness check.
-   [ ] Configure bounded retries and failure alerts.
-   [ ] Demonstrate a critical quality failure blocking publication.
-   [ ] Demonstrate safe recovery from a transient failure.
-   [ ] Configure CloudWatch monitoring and inspect logs.
-   [ ] Add a runbook and evidence for a successful end-to-end run.
-   [ ] Document which parts are implemented, pending, or intentionally
    optional.

## 19. Current implementation status

This status reflects the latest known checkpoint and must be updated
after each implementation milestone.

  -----------------------------------------------------------------------
  Component               Current known status    Evidence needed to mark
                                                  complete
  ----------------------- ----------------------- -----------------------
  PostgreSQL / RDS        Connection tested;      Re-run connectivity and
                          source tables created   schema checks

  DMS endpoints           Endpoint tests          Endpoint test results
                          succeeded               and configuration
                                                  record

  DMS full load           Not yet verified; task  Successful task/table
                          previously showed start status, loaded counts,
                          required / 0% and table and S3 objects
                          errors                  

  DMS CDC                 Not yet verified        Controlled
                                                  insert/update/delete
                                                  test reflected
                                                  correctly in target

  S3 Bronze               Target bucket/prefix    Real DMS output objects
                          configured; successful  and documented format
                          ingestion still needs   
                          verification            

  Glue / PySpark          Planned / to implement  Successful job run,
                                                  logs, and output
                                                  validation

  Silver / Gold           Planned / to implement  Output datasets and
                                                  passing quality tests

  Athena                  Planned / to implement  Successful SQL queries
                                                  against cataloged
                                                  datasets

  Redshift                Planned / to implement  Created schema/tables,
                                                  successful load,
                                                  reconciliation

  Airflow DAG             Planned / to implement  DAG run with
                                                  dependencies, retry and
                                                  failure behavior tested

  Lambda                  Optional / planned only Working small
                          if justified            event-driven use case
                                                  and test evidence

  IAM / CloudWatch        Requires review and     Least-privilege
                          configuration evidence  policies, logs, and
                          per service             relevant alarms

  Power BI                Planned / to implement  Report connected to
                          if available            approved curated
                                                  Redshift objects
  -----------------------------------------------------------------------

## 20. Resume project description

**Use the version that accurately reflects the work completed. Do not
claim that planned services are already running.**

### Wording while implementation is in progress

**Healthcare & Pharma Data Platform \| AWS, PostgreSQL, PySpark, Apache
Spark**

-   Designed a medallion-style data engineering architecture to ingest
    healthcare entities from PostgreSQL on Amazon RDS into Amazon S3,
    with AWS DMS planned for initial load and CDC replication.
-   Created relational source tables for patients, doctors,
    appointments, treatments, and billing, and validated key
    relationships in the sample dataset.
-   Building AWS Glue/PySpark transformations for data cleansing, schema
    standardization, deduplication, referential-integrity checks, and
    Silver/Gold data layers.
-   Designing fact and dimension tables for analytics, with Athena for
    data-lake SQL validation and Redshift as the intended BI-serving
    layer.
-   Defining Airflow orchestration, data-quality gates, retry/recovery
    handling, IAM access controls, and CloudWatch monitoring.

Once a component is implemented, tested, and supported by evidence,
revise the bullet to say what you actually built and validated. Avoid
using "production experience" or "production deployment" for a portfolio
environment.

## 21. Interview walkthrough

A clear project explanation should follow the data flow, not just list
service names.

> "I am building a healthcare data platform using PostgreSQL on Amazon
> RDS as the source. The source contains patients, doctors,
> appointments, treatments, and billing data. AWS DMS is configured to
> replicate the source to S3, with a full load followed by CDC once the
> task is validated. The raw landing area is the Bronze layer. AWS Glue
> runs PySpark code on Apache Spark to standardize schemas, clean
> records, validate keys and relationships, and produce Silver datasets.
> I then model the data into Gold fact and dimension tables with a
> defined grain to avoid duplicate counting and inflated billing totals.
> Athena is used for SQL investigation and data-quality validation on
> S3, while Redshift is the intended analytical serving layer for Power
> BI. Airflow coordinates the dependencies and schedule, applies bounded
> retries, and prevents publication when critical checks fail. IAM
> controls access and CloudWatch is used for logs and monitoring. I am
> validating each component step by step and can show the actual run
> evidence for the components completed."

Be prepared to explain the status of each service honestly. A recruiter
may ask for the exact DMS task state, a sample PySpark transformation, a
failed-job example, the quality rules, a star-schema diagram, or
evidence of a Redshift/Power BI connection. Show only what has actually
been implemented.

## 22. Definition of done

The project is ready to present as an end-to-end working portfolio
project when you can demonstrate all of the following:

-   A successful DMS full load and a controlled CDC test.
-   Actual S3 Bronze data, not only empty prefixes.
-   A successful Glue/PySpark run producing Silver outputs.
-   Demonstrated data-quality checks, quarantine behavior, and failure
    gates.
-   Correct Gold fact/dimension models with documented grain and
    reconciled measures.
-   Athena queries that validate the lake outputs.
-   A repeatable Redshift load with reconciled counts/totals.
-   An Airflow DAG with tested dependencies, schedule, bounded retries,
    and recovery behavior.
-   CloudWatch logs and appropriate monitoring.
-   A Power BI report or a documented, tested alternative if Power BI
    connectivity is unavailable.
-   A README and runbook that distinguish implemented features from
    planned ones.

------------------------------------------------------------------------

## Key design principles

1.  **Do not publish unvalidated data.**
2.  **Do not advance a CDC checkpoint before the corresponding output is
    safely committed and verified.**
3.  **Do not retry deterministic permission or schema errors without
    fixing them.**
4.  **Do not join facts at different grains without checking for row
    multiplication.**
5.  **Do not grant broad permissions merely to make a job pass.**
6.  **Do not describe a design as a working implementation until you
    have tested it.**
7.  **Keep raw healthcare data restricted; expose only approved
    reporting data to BI users.**
8.  **Capture run evidence so you can explain not only the architecture,
    but also what you built, what failed, how you diagnosed it, and how
    you recovered.**

This README is the implementation blueprint for the project. Update the
status table as each phase is completed so the document remains both
recruiter-friendly and technically honest.
