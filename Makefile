.PHONY: up down logs ps test psql data prepare replay backfill

NYC_MONTH ?= 2026-09
TLC = https://d37ci6vzurychx.cloudfront.net

.env:
	cp .env.example .env

up: .env
	docker compose up -d --build

down:
	docker compose down

logs:
	docker compose logs -f --tail=100

ps:
	docker compose ps -a

test:
	uv run pytest -q

psql:
	docker compose exec timescaledb psql -U surge -d surge

# Boston Kaggle CSVs are downloaded manually into data/raw/boston/ (see README).
data:
	mkdir -p data/raw/nyc data/raw/boston data/processed
	test -f data/raw/nyc/taxi_zones.zip || curl -sSfL -o data/raw/nyc/taxi_zones.zip $(TLC)/misc/taxi_zones.zip
	test -f data/raw/nyc/fhvhv_tripdata_$(NYC_MONTH).parquet || curl -SfL -o data/raw/nyc/fhvhv_tripdata_$(NYC_MONTH).parquet $(TLC)/trip-data/fhvhv_tripdata_$(NYC_MONTH).parquet
	@for f in rideshare_kaggle.csv cab_rides.csv weather.csv; do test -f data/raw/boston/$$f || echo "missing data/raw/boston/$$f (download from Kaggle)"; done

prepare: data
	uv run python -m producers.replay.prepare_nyc --month $(NYC_MONTH)

replay:
	uv run python -m producers.replay.replay --file data/processed/nyc_fhvhv_$(NYC_MONTH).parquet $(ARGS)

backfill:
	uv run python -m producers.backfill_weather $(ARGS)
