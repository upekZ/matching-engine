.PHONY: build run-server run-tests

include .env

build:
	@echo 'Building..'
	go build -o cmd/api/main cmd/api/main.go
	@echo 'Built and saved to: cmd/api/main'

run-server:
	go run cmd/api/main.go

run-tests:
	go test -v ./tests