VERSION ?= 0.1.0
SERVICES := sso notifier chat gateway
COMPOSE := docker compose -f deployments/docker-compose.yml --project-directory .
KIND_CLUSTER := messenger

.PHONY: certs up down logs run-sso run-notifier run-chat run-gateway test lint proto images kind-up kind-load k8s-apply k8s-nginx-reload kind-down

# Самоподписанный TLS-сертификат для nginx (только для локалки)
certs:
	mkdir -p deployments/nginx/certs
	openssl req -x509 -newkey rsa:2048 -nodes -days 365 \
		-subj "/CN=localhost" -addext "subjectAltName=DNS:localhost" \
		-keyout deployments/nginx/certs/tls.key -out deployments/nginx/certs/tls.crt

# Весь стек в Docker Compose
up:
	$(COMPOSE) up -d --build

down:
	$(COMPOSE) down

logs:
	$(COMPOSE) logs -f sso notifier chat gateway nginx

# Запуск сервиса локально через go run (инфраструктура из compose)
run-sso:
	set -a && . ./configs/sso.env && set +a && go run ./cmd/sso

run-notifier:
	set -a && . ./configs/notifier.env && set +a && go run ./cmd/notifier

run-chat:
	set -a && . ./configs/chat.env && set +a && go run ./cmd/chat

run-gateway:
	set -a && . ./configs/gateway.env && set +a && go run ./cmd/gateway

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
	kubectl -n messenger create configmap nginx-templates \
		--from-file=deployments/nginx/templates/ --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n messenger create configmap nginx-web \
		--from-file=web/ --dry-run=client -o yaml | kubectl apply -f -
	kubectl -n messenger create secret tls nginx-tls \
		--cert=deployments/nginx/certs/tls.crt --key=deployments/nginx/certs/tls.key \
		--dry-run=client -o yaml | kubectl apply -f -
	kubectl apply -f deployments/k8s/

# После правки шаблона: обновить ConfigMap и перезапустить nginx
k8s-nginx-reload: k8s-apply
	kubectl -n messenger rollout restart deployment/nginx

kind-down:
	kind delete cluster --name $(KIND_CLUSTER)
