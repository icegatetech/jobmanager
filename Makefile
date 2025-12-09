.PHONY: examples-infra-up examples-infra-down tests

examples-infra-up:
	cd ./_examples && docker compose up --detach

examples-infra-down:
	cd ./_examples && docker compose down

tests:
	go test -v -race ./...