.PHONY: test lint-ui build-ui ci check-format

GOCMD=go
GOTEST=$(GOCMD) test
GOBUILD=$(GOCMD) build

UI_DIR=dashboard/surge-dashboard

test:
	$(GOTEST) -v ./...

lint-ui:
	cd $(UI_DIR) && npm install && npm run lint

build-ui:
	cd $(UI_DIR) && npm install && npm run build

ci: test lint-ui build-ui
