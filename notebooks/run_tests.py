# Databricks notebook source
# MAGIC %md
# MAGIC # Unit tests
# MAGIC Runs every test in `tests/` with **pytest** on the small sample file `data/sample_rides.csv`.
# MAGIC Tests build DataFrames in memory, so they never touch the production Bronze/Silver/Gold tables.
# MAGIC
# MAGIC **Screenshot for the report:** the final pytest summary line (`N passed`).

# COMMAND ----------

import os, sys
sys.dont_write_bytecode = True          # workspace files cannot hold __pycache__ reliably

notebook_dir = os.getcwd()              # .../uber-taxi-agentic-etl/notebooks
repo_root = os.path.dirname(notebook_dir)
if repo_root not in sys.path:
    sys.path.insert(0, repo_root)
print("Repo root:", repo_root)

# COMMAND ----------

import pytest

exit_code = pytest.main([
    os.path.join(repo_root, "tests"),
    "-v",                      # one line per test
    "-p", "no:cacheprovider",  # do not write .pytest_cache into the repo
    "--tb=short",
])
print("pytest exit code:", exit_code, "(0 = all tests passed)")
assert exit_code == 0, "Some tests failed - see the output above"
