.PHONY: run tidy build

run:
	GO111MODULE=on CONFIG_PATH=./config/dev.yaml go run ./backend

tidy:
	go mod tidy

build:
	GO111MODULE=on go build -o bin/sr-ingest ./backend