tws: *.go
	go clean
	go vet
	golint
	go build
	go install

clean:
	go clean
	rm -rf *.out *.log

test:
	go test -coverprofile=coverage.out
	go tool cover -html=coverage.out

all: clean tws test

