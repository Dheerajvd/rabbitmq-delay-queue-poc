.PHONY: infra-up infra-down backend frontend test test-integration

-include .env
export GOTOOLCHAIN AMQP_URL HTTP_ADDR SQLITE_PATH FIXED_DELAY_MINUTES PROCESSING_DURATION_SECONDS RABBITMQ_AMQP_PORT RABBITMQ_MANAGEMENT_PORT

infra-up:
	docker compose up -d rabbitmq
	./scripts/wait-for-rabbitmq.sh

infra-down:
	docker compose down

backend:
	cd backend && go run ./cmd/server

frontend:
	cd frontend && npm install && npm run dev

test:
	cd backend && go test ./...

test-integration:
	cd backend && go test -tags=integration -count=1 -timeout 240s ./...
