.PHONY: data

DATA_URL = "https://data.cityofnewyork.us/api/views/usrf-za7k/rows.csv?accessType=DOWNLOAD"
DATA_FILE = data/good_eats.csv

data:
	mkdir -p data
	wget -O $(DATA_FILE) $(DATA_URL)
# Run the test suite
test:
	poetry run pytest

# Format the code using Black
format:
	poetry run black .

# Lint the code using Flake8
lint:
	poetry run flake8 .

# Run all quality checks: formatting, linting, and tests
check: format lint test
