all: dgraph-stress-test

dgraph-stress-test: *.go
	go build -o $@ $^

clean:
	rm -f dgraph-stress-test

.phony: all clean