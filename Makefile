.PHONY: build run int-test unit-test clean clean-test

RUN_LOCAL ?= false
DOCKER_COMPOSE = docker compose -f integration/docker-compose.yml
DOCKER_COMPOSE_TEST = docker compose -f integration/docker-compose-test.yml
APP_SERVICE = app
TEST_SERVICE = matching-engine-integration
UNIT_TEST_SERVICE = matching-engine-unit-tests

build:
ifeq ($(RUN_LOCAL),false)
	$(DOCKER_COMPOSE) build $(APP_SERVICE)
else
	go build -o matching-engine cmd/api/main.go
endif

run: clean
ifeq ($(RUN_LOCAL),false)
	@echo "Starting application in foreground. Press Ctrl+C to stop and clean up."
	$(DOCKER_COMPOSE) up --build $(APP_SERVICE); \
	$(DOCKER_COMPOSE) down --volumes
else
	./matching-engine
endif

int-test: clean-test
ifeq ($(RUN_LOCAL),false)
	$(DOCKER_COMPOSE_TEST) up --build --abort-on-container-exit $(TEST_SERVICE); \
	$(DOCKER_COMPOSE_TEST) down --volumes
	@echo "Integration tests completed. Check logs with 'docker logs matching-engine-$(TEST_SERVICE)-1'"
else
	go test -v ./tests/integration_test.go
endif

unit-test: clean-test
ifeq ($(RUN_LOCAL),false)
	$(DOCKER_COMPOSE_TEST) up --build --abort-on-container-exit $(UNIT_TEST_SERVICE); \
	$(DOCKER_COMPOSE_TEST) down --volumes
	@echo "Unit tests completed. Check logs with 'docker logs matching-engine-$(UNIT_TEST_SERVICE)-1')"
else
	go test -v ./internal/engine
endif

clean:
ifeq ($(RUN_LOCAL),false)
	$(DOCKER_COMPOSE) down --volumes --remove-orphans
	@echo "Docker containers and volumes cleaned up"
else
	rm -f matching-engine
	@echo "Local binary cleaned up"
endif

clean-test:
ifeq ($(RUN_LOCAL),false)
	$(DOCKER_COMPOSE_TEST) down --volumes --remove-orphans
	@echo "Test Docker containers and volumes cleaned up"
else
	@echo "No test containers to clean in local mode"
endif