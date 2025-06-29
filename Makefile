.PHONY: build start start-unit-tests stop int-test unit-test clean clean-test

DOCKER_COMPOSE = docker compose -f integration/docker-compose.yml
APP_SERVICE = app
UNIT_TEST_SERVICE = matching-engine-unit-tests

build:
	$(DOCKER_COMPOSE) build $(APP_SERVICE)

start:
	@echo "Starting application in background..."
	$(DOCKER_COMPOSE) up -d --build $(APP_SERVICE)

start-unit-tests:
	@echo "Starting unit test container in background..."
	$(DOCKER_COMPOSE) up -d --build $(UNIT_TEST_SERVICE)

stop:
	@echo "Stopping application and unit test containers..."
	$(DOCKER_COMPOSE) stop

int-test: start
	@echo "Running integration tests in app service..."
	$(DOCKER_COMPOSE) exec $(APP_SERVICE) sh -c "go test -v ./internal/matching_test"
	@echo "Integration tests completed. Check logs with 'docker logs matchingEngineApp'"

unit-test: start-unit-tests
	@echo "Running unit tests in unit test service..."
	$(DOCKER_COMPOSE) exec $(UNIT_TEST_SERVICE) sh -c "go test -v ./internal/engine ./internal/storage/redis-store"
	@echo "Unit tests completed. Check logs with 'docker logs matching-engine-$(UNIT_TEST_SERVICE)-1'"

clean:
	$(DOCKER_COMPOSE) down --volumes --remove-orphans
	@echo "Docker containers and volumes cleaned up"

clean-test:
	$(DOCKER_COMPOSE) down --volumes --remove-orphans
	@echo "Test Docker containers and volumes cleaned up"