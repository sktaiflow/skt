all:
	gcloud builds submit --tag gcr.io/sktaic-datahub/services/etl/etl-api:1.0.1 .
test:
	docker build --no-cache -t etl-api .
	docker run -p 80:80 -it etl-api