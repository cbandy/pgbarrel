
.PHONY: test
test:
	go test

vendor: *.go
	@command -v vendetta > /dev/null || go get github.com/dpw/vendetta
	vendetta -p -n github.com/cbandy/pgbarrel
