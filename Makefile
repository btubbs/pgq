.PHONY: tests viewcoverage check dep ci

GOBIN ?= $(GOPATH)/bin

all: tests check

dep: $(GOBIN)/dep
	$(GOBIN)/dep ensure -v

tests: dep
	go test .

coverage.txt: dep $(GOBIN)/go-acc
	go test -coverprofile=$@

viewcoverage: coverage.txt 
	go tool cover -html=$<

vet:
	go vet ./...

check: $(GOBIN)/golangci-lint
	$(GOBIN)/golangci-lint run

$(GOBIN)/goveralls:
	go get -v -u github.com/mattn/goveralls

$(GOBIN)/dep:
	go get -v -u github.com/golang/dep/cmd/dep

$(GOBIN)/golangci-lint:
	curl -sfL https://i$(GOBIN)/go-accnstall.goreleaser.com/github.com/golangci/golangci-lint.sh | sh -s -- -b $(GOPATH)/bin v1.12.3

$(GOBIN)/go-acc:
	go get github.com/ory/go-acc

ci: coverage.txt vet check $(GOBIN)/goveralls
	$(GOBIN)/goveralls -coverprofile=$< -service=travis-ci
