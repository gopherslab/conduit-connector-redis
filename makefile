.PHONY: build test lint clean

build:
	go build -o conduit-connector-redis cmd/main.go

test:
	go test $(GOTEST_FLAGS) -race ./...

lint:
	golangci-lint run ./... -v

clean:
	golangci-lint cache clean
	go clean -testcache
	rm conduit-connector-redis