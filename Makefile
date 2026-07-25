.PHONY: build build-web build-go run test lint docker-up docker-down

## Build the dashboard, then the aegisd/aegis binaries that embed it.
build: build-web build-go

build-web:
	cd web && npm install && npm run build

build-go:
	go build -o bin/aegisd ./cmd/aegisd
	go build -o bin/aegis ./cmd/aegis

## Run the server against a local SQLite file for quick manual testing.
run: build-web
	go run ./cmd/aegisd

test:
	go test ./... -race -count=1

lint:
	gofmt -l .
	go vet ./...
	cd web && npm run build

docker-up:
	docker compose up --build

docker-down:
	docker compose down
