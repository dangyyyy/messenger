VERSION ?= 0.1.0
SERVICES := sso notifier
COMPOSE := docker compose -f deployments/docker-compose.yml --project-directory .
KIND_CLUSTER := messenger

.PHONY: up down logs run-sso run-notifier test lint proto images kind-up kind-load k8s-apply kind-down

# Весь стек в Docker Compose
up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f sso notifier

# Запуск сервиса локально через go run (инфраструктура из compose)
run-sso:
	set -a && . ./configs/sso.env && set +a && go run ./cmd/sso

run-notifier:
	set -a && . ./configs/notifier.env && set +a && go run ./cmd/notifier

test:
	go test ./... -race -count=1

lint:
	golangci-lint run
	buf lint

proto:
	buf generate

images:
	$(foreach s,$(SERVICES),docker build -f build/package/$(s)/Dockerfile -t messenger/$(s):$(VERSION) . &&) true

# Kubernetes в kind
kind-up:
	kind create cluster --config deployments/kind.yaml

kind-load: images
	$(foreach s,$(SERVICES),kind load docker-image messenger/$(s):$(VERSION) --name $(KIND_CLUSTER) &&) true

k8s-apply:
	kubectl apply -f deployments/k8s/namespace.yaml
	kubectl apply -f deployments/k8s/

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)
