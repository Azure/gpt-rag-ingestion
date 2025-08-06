# Running Pytest Unit Tests Locally

This project uses [Pytest](https://docs.pytest.org/) to run unit tests, and is configured to run them automatically on pull requests to the `develop` and `main` branches using GitHub Actions. You can also run these tests locally to verify your changes before pushing them.

## 🧪 Test Summary

Tests are run using:
- Python 3.11
- Dependencies listed in `requirements-dev.txt`
- Test files located in the `./tests` directory

## ✅ Prerequisites

Before running the tests locally, ensure you have the following installed:

- Python 3.11
- [pip](https://pip.pypa.io/)
- [virtualenv (optional but recommended)](https://virtualenv.pypa.io/)

## ⚙️ Setup

1. **Clone the repository:**

   ```bash
   git clone https://github.com/Salesfactory/gpt-rag-ingestion.git
   cd gpt-rag-ingestion

   ```
2. **Create and activate a virtual environment:**
   ```bash
     python -m venv venv
     source venv/bin/activate  # On Windows use: venv\Scripts\activate
   ```
3. **Install development dependencies**
   ```bash 
     pip install -r requirements-dev.txt
   ```

## ▶️ Running the Tests
To run the test suite locally:
   ```bash 
     python -m pytest -v ./tests
   ```
This will execute all unit tests in the tests directory and display a summary of the results.