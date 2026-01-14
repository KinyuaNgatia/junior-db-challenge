.PHONY: run-repl run-web test clean

run-repl:
	go run cmd/repl/main.go

run-web:
	go run cmd/web/main.go

test:
	go test ./...

clean:
	rm -rf data/*.json
