# Contributing to Universal Data Engineering Pipeline

Thank you for your interest in contributing to the **Universal Data Engineering Pipeline**! We welcome bug reports, documentation enhancements, feature proposals, and code contributions.

---

## Code of Conduct

This project and everyone participating in it is governed by our [Code of Conduct](CODE_OF_CONDUCT.md). By participating, you are expected to uphold this code.

---

## Development Workflow

### 1. Fork and Clone
```bash
git clone https://github.com/<your-username>/universal-data-engineering-pipeline.git
cd universal-data-engineering-pipeline
```

### 2. Set Up Virtual Environment
```bash
# Create virtual environment
python -m venv .venv

# Activate on Linux / macOS:
source .venv/bin/activate

# Activate on Windows (PowerShell):
.venv\Scripts\Activate.ps1
```

### 3. Install Dependencies
```bash
pip install -r requirements.txt
```

### 4. Configure Environment
```bash
cp .env.example .env
```
*(Optionally configure `DATABASE_URL` with your PostgreSQL database credentials; if unconfigured, all stages automatically fall back to local validated caches and SQLite).*

### 5. Running the Pipeline and Dashboard
```bash
# Execute end-to-end pipeline
python run_pipeline.py

# Launch interactive web dashboard
python run_dashboard.py --port 5000
```

---

## Running Automated Tests

All changes must pass the full automated test suite before opening a pull request:

```bash
python -m unittest discover tests
```

To run a specific test module:
```bash
python -m unittest tests.test_pipeline
python -m unittest tests.test_dashboard
python -m unittest tests.test_database
```

---

## Submitting Pull Requests

1. **Create a branch**: `git checkout -b feature/your-feature-name` or `bugfix/issue-description`
2. **Make your changes**: Adhere to PEP 8 standards, include docstrings, and write unit tests for new logic.
3. **Verify tests pass**: Ensure all tests pass locally.
4. **Commit with clear messages**: Write descriptive, imperative commit messages (e.g., `Add Parquet ingestion support to Stage 2`).
5. **Open a Pull Request**: Provide a clear summary of your changes, reference any related issues, and ensure CI passes.

---

## Code Style Guidelines

- Python 3.10+ type hinting (`from __future__ import annotations`, `dict[str, Any]`, `list[str]`).
- Comprehensive docstrings for classes and public methods.
- Domain-agnostic design: Avoid hardcoding assumptions about specific domain schemas (e.g., e-commerce, banking).
- Safe credential handling: Never log or print unmasked database credentials or secrets.
