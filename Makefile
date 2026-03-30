.PHONY: build run full dev linux clean

# Build for current OS
build:
	go build -o syncd ./cmd/syncd

# Build for Linux (VPS deployment)
linux:
	GOOS=linux GOARCH=amd64 go build -o syncd ./cmd/syncd

# Run incremental sync (continuous)
run: build
	./syncd

# Run full sync (one-time)
full: build
	./syncd full

# Run with .env loaded
dev:
	go run ./cmd/syncd

# Run full sync with .env
dev-full:
	go run ./cmd/syncd full

# Clean
clean:
	rm -f syncd syncd.exe

# Install dependencies
deps:
	go mod tidy
