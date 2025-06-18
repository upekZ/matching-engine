.PHONY: build run int-test unit-test clean

USE_DOCKER ?= false
DOCKER_COMPOSE = docker compose
DOCKER_COMPOSE_TEST = docker compose -f docker-compose-test.yml
APP_SERVICE = app
TEST_SERVICE = matching-engine-integration
UNIT_TEST_SERVICE = matching-engine-unit-tests

build:
ifeq ($(USE_DOCKER),true)
	$(DOCKER_COMPOSE) build $(APP_SERVICE)
else
	go build -o matching-engine cmd/api/main.go
endif

run:
ifeq ($(USE_DOCKER),true)
	$(DOCKER_COMPOSE) up --build -d $(APP_SERVICE)
	@echo "Application running. Check logs with 'docker logs matching-engine-$(APP_SERVICE)-1'"
else
	./matching-engine
endif

int-test:
ifeq ($(USE_DOCKER),true)
	$(DOCKER_COMPOSE_TEST) down
	$(DOCKER_COMPOSE_TEST) up --build $(TEST_SERVICE)
	@echo "Integration tests completed. Check logs with 'docker logs matching-engine-$(TEST_SERVICE)-1'"
else
	go test -v ./tests/integration_test.go
endif

unit-test:
ifeq ($(USE_DOCKER),true)
	$(DOCKER_COMPOSE_TEST) down
	$(DOCKER_COMPOSE_TEST) up --build $(UNIT_TEST_SERVICE)
	@echo "Unit tests completed. Check logs with 'docker logs matching-engine-$(UNIT_TEST_SERVICE)-1'"
else
	go test -v ./internal/engine
endif

clean:
ifeq ($(USE_DOCKER),true)
	$(DOCKER_COMPOSE) down
	$(DOCKER_COMPOSE_TEST) down
	@echo "Docker containers cleaned up"
else
	rm -f matching-engine
	@echo "Local binary cleaned up"
endif