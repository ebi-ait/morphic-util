.PHONY: build check clean test publish service-build service-run service-test

build:
	@echo "build"
	python setup.py sdist bdist_wheel

check:
	twine check dist/*

clean:
	@echo "clean"
	rm -rf dist/*
	rm -rf build/*

test:
	@echo "test"
	nosetests

publish:
	@echo "publish"
	twine upload dist/*

# --- HTTP /validate service ---------------------------------------------------
# Build the Docker image. Build context is the repository root so that the
# Dockerfile can `pip install .[service]`.
SERVICE_IMAGE ?= morphic-util-service:latest

service-build:
	@echo "building $(SERVICE_IMAGE)"
	docker build -f service/Dockerfile -t $(SERVICE_IMAGE) .

# Run the service locally with hot reload (no Docker). Requires the service
# extra to be installed: `pip install -e .[service]`.
service-run:
	@echo "running service on http://localhost:8000"
	uvicorn service.app:app --host 0.0.0.0 --port 8000 --reload

service-test:
	@echo "running service tests"
	pytest service/tests
