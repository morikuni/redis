.PHONY: test
test:
	go test -v -race -count 1 ./...

.PHONY: coverage
coverage:
	go test -v -race -count 1 -covermode=atomic -coverprofile=coverage.out ./...

.PHONY: bench
bench:
	docker-compose up -d
	REDIS_ADDR=localhost:6379 go test -v -bench . -benchmem -count 1 -benchtime 5s ./... || true
	docker-compose down
