.PHONY: build run int-test unit-test clean clean-test

DOCKER_COMPOSE = docker compose -f integration/docker-compose.yml
APP_SERVICE = app
TEST_SERVICE = matching-engine-integration
UNIT_TEST_SERVICE = matching-engine-unit-tests

build:
	$(DOCKER_COMPOSE) build $(APP_SERVICE)

run: clean
	@echo "Starting application in foreground. Press Ctrl+C to stop and clean up."
	$(DOCKER_COMPOSE) up --build $(APP_SERVICE); \
	$(DOCKER_COMPOSE) down --volumes

int-test: clean-test
	$(DOCKER_COMPOSE) up --build --abort-on-container-exit $(TEST_SERVICE); \
	$(DOCKER_COMPOSE) down --volumes
	@echo "Integration tests completed. Check logs with 'docker logs matching-engine-$(TEST_SERVICE)-1'"

unit-test: clean-test
	$(DOCKER_COMPOSE) up --build --abort-on-container-exit $(UNIT_TEST_SERVICE); \
	$(DOCKER_COMPOSE) down --volumes
	@echo "Unit tests completed. Check logs with 'docker logs matching-engine-$(UNIT_TEST_SERVICE)-1')"

clean:
	$(DOCKER_COMPOSE) down --volumes --remove-orphans
	@echo "Docker containers and volumes cleaned up"

clean-test:
	$(DOCKER_COMPOSE) down --volumes --remove-orphans
	@echo "Test Docker containers and volumes cleaned up"