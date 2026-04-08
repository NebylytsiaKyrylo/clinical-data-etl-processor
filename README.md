# Clinical Data ETL Processor

A Python-based Extract-Transform-Load (ETL) pipeline for integrating clinical health data with supplement usage
information. This project processes multi-source health datasets into a unified, analysis-ready format.

## 1. Context

Health data from wearable devices and supplement usage logs exist in separate data silos. Clinicians and data scientists
need to cross-reference these sources manually, which is cumbersome and error-prone.

This project builds an automated **ETL pipeline** to:

- **Extract**: Load four CSV sources (health metrics, supplement usage, user profiles, experiments)
- **Transform**: Clean, validate, and standardize data (type conversions, missing value handling, unit normalization)
- **Merge**: Combine datasets into a single, comprehensive view
- **Load**: Export analysis-ready CSV for downstream analytics

The pipeline is fully tested, documented, and production-ready.

## 2. Objective

Deliver a clean, unified dataset combining:

- User health metrics (heart rate, glucose, sleep, activity)
- Supplement intake logs with dosage and experiment association
- User demographics and experiment metadata

Output columns are strictly defined and validated to ensure data quality for analytics workflows.

## 3. Architecture and Tech Stack

- **Python 3.14+**
- **Pandas 3.0+**
- **NumPy 2.4+**
- **uv** - Package manager
- **pytest 9.0+**
- **ruff** — Code quality, linting, and formatting

## 4. Data Catalog

### **Sources**

| File                                                  | Description                                                                       |
|-------------------------------------------------------|-----------------------------------------------------------------------------------|
| [user_health_data.csv](data/raw/user_health_data.csv) | Daily health metrics from wearable devices (heart rate, glucose, sleep, activity) |
| [supplement_usage.csv](data/raw/supplement_usage.csv) | Supplement intake logs with dosage and experiment association                     |
| [experiments.csv](data/raw/experiments.csv)           | Experiment metadata and names                                                     |
| [user_profiles.csv](data/raw/user_profiles.csv)       | User demographics (ID, age, email)                                                |

### **Output: processed_data.csv**

| Column               | Type     | Notes                                             |
|----------------------|----------|---------------------------------------------------|
| `user_id`            | string   | No missing values                                 |
| `date`               | datetime | No missing values                                 |
| `email`              | string   | No missing values                                 |
| `user_age_group`     | string   | "Under 18", "18-25", ..., "Over 65", or "Unknown" |
| `experiment_name`    | string   | May be missing for health-only entries            |
| `supplement_name`    | string   | "No intake" for days without supplementation      |
| `dosage_grams`       | float    | Converted from mg to grams                        |
| `is_placebo`         | boolean  | May be missing for "No intake" days               |
| `average_heart_rate` | float    | May be missing                                    |
| `average_glucose`    | float    | May be missing                                    |
| `sleep_hours`        | float    | May be missing                                    |
| `activity_level`     | float    | Clipped to [0, 100] range                         |

## 5. The ETL Pipeline

The pipeline is split into dedicated, fully typed and logged modules:

| Phase       | Module                                       | Function                  | Purpose                                                            |
|-------------|----------------------------------------------|---------------------------|--------------------------------------------------------------------|
| Extract     | [scripts/extract.py](scripts/extract.py)     | `extract_csv()`           | Reads CSV files from `data/raw/`, handles missing files gracefully |
| Transform   | [scripts/transform.py](scripts/transform.py) | `transform_profiles()`    | Categorizes users into 7 age groups, removes invalid rows          |
|             |                                              | `transform_health_data()` | Converts dates, cleans sleep_hours, clips activity_level           |
|             |                                              | `transform_supl_usage()`  | Converts dates, creates dosage_grams column                        |
| Merge       |                                              | `merge_data()`            | Combines all datasets, fills missing supplements with "No intake"  |
| Load        | [scripts/load.py](scripts/load.py)           | `save_csv()`              | Writes merged DataFrame to CSV                                     |
| Orchestrate | [main.py](main.py)                           | `merge_all_data()`        | Entry point that chains all phases                                 |

## 6. Project Structure

```text
.
|-- data/
|   |-- raw/                              # Input CSV files
|   |   |-- user_health_data.csv
|   |   |-- supplement_usage.csv
|   |   |-- experiments.csv
|   |   └── user_profiles.csv
|   └── processed/                        # Output merged dataset
|
|-- scripts/
|   |-- extract.py                        # CSV data extraction
|   |-- transform.py                      # Data cleaning & transformation
|   └── load.py                           # CSV output writing
|
|-- tests/
|   |-- conftest.py                       # Pytest fixtures and configuration
|   |-- test_extract.py                   # Extract module tests
|   |-- test_transform.py                 # Transform module tests
|   |-- test_load.py                      # Load module tests
|   └── test_main.py                      # Integration tests
|
|-- docs/
|   |-- requirements.md                   # Data specifications
|   └── schema.png                        # Database schema diagram
|
|-- main.py                               # Pipeline entry point
|-- config.py                             # Configuration paths
|-- pyproject.toml                        # Project metadata & dependencies
└── README.md                             
```

## 7. Installation & Setup

### 7.1 Prerequisites

- Python 3.14 or higher
- [uv](https://docs.astral.sh/uv/) (fast Python package manager)

### 7.2 Setup

1. Clone the repository:

```bash
git clone https://github.com/NebylytsiaKyrylo/clinical-data-etl-processor.git
cd clinical-data-etl-processor
```

2. Create a virtual environment and install dependencies using `uv`:

```bash
# Create virtual environment (uv handles this automatically)
uv venv

# Activate the environment
source .venv/bin/activate  # On Windows: .venv\Scripts\activate

# Install dependencies
uv pip install -e .

# Install development dependencies (for testing)
uv pip install -e ".[dev]"
```

That's it! All dependencies are now installed.

## 8. Usage

### 8.1 Run the Pipeline

```bash
python main.py
```

This executes the full ETL pipeline:

1. Extracts data from 4 CSV files in `data/raw/`
2. Transforms and cleans each dataset
3. Merges all data into a single unified dataset
4. Saves the result to `data/processed/processed_data.csv`

### 8.2 Output

After running, check `data/processed/processed_data.csv` for the merged dataset.

## 9. Testing

### 9.1 Run All Tests

```bash
pytest tests/ -v
```

### 9.2 Run Specific Test File

```bash
pytest tests/test_extract.py -v
pytest tests/test_transform.py -v
pytest tests/test_load.py -v
pytest tests/test_main.py -v
```

### 9.3 About conftest.py

[conftest.py](tests/conftest.py) is pytest's automatic configuration file. It:

- **Configures sys.path** so imports like `from scripts.extract import ...` work correctly
- **Provides shared fixtures** available to all test files without importing:
    - `sample_profiles_df` — DataFrame with valid user profile data
    - `sample_health_df` — DataFrame with health metrics and sleep hours in raw format
    - `sample_usage_df` — DataFrame with supplement usage data
    - `sample_experiments_df` — DataFrame with experiment metadata

Fixtures reduce code duplication and ensure consistent test data across all test files.

## 10. Code Quality

This project enforces strict code quality standards using [ruff](https://docs.astral.sh/ruff/).

### 10.1 Check Code Quality

```bash
# Check for linting and style issues
ruff check scripts/ main.py
```

### 10.2 Auto-fix Issues

```bash
# Automatically fix fixable issues
ruff check --fix scripts/ main.py

# Or use the format command for style fixes
ruff format scripts/ main.py
```

### 10.3 Configuration

Ruff is configured in [pyproject.toml](pyproject.toml) with 70+ enabled rules covering:

- Code style and formatting (E, W)
- Python best practices (B, F, PL)
- Type hints and annotations (ANN)
- Docstring standards (D)
- Security checks (S)
- And many more...

Run `ruff check --fix` before committing to ensure code quality.

## 11. Error Handling

The pipeline handles common data issues gracefully:

- **Missing files** → Returns empty DataFrame, logs error
- **Malformed CSV** → Caught and handled, returns empty DataFrame
- **Missing values** → Removed for critical fields (user_id, email, date), retained for optional fields
- **Type mismatches** → Converted or coerced to correct types
- **Invalid ranges** → Clipped to valid bounds (e.g., activity_level [0, 100])

All operations are logged at INFO and ERROR levels for debugging.

