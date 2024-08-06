COMMIT := $(shell git rev-parse --short=8 HEAD)
DOCKER_IMAGE := fidays/airline-scraper:$(COMMIT)
KUBE_MANIFEST := kubernetes/deployment.yaml
NAMESPACE := airline-engine



build:
	docker build -t finkraft/bulk-downloader:latest . 

exec:
	docker run --rm -p 5000:5000 -it finkraft/bulk-downloader:latest /bin/bash

run:
	docker run --rm -p 5000:5000 -it finkraft/bulk-downloader:latest 