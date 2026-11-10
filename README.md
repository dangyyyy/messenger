# messenger

Микросервисный мессенджер на Go: регистрация, личные чаты, сообщения в реальном времени через WebSocket, веб-интерфейс.
gRPC, REST (grpc-gateway), PostgreSQL, Redis (кэш, rate limit, pub/sub), Kafka (transactional outbox), Nginx, Docker, Kubernetes.

## Архитектура

```
браузер ──HTTPS──▶ nginx ─┬─ /         статика (web/index.html)
                          ├─ /api/     gateway (REST → gRPC) ─┬─▶ sso
                          │                                   └─▶ chat
                          ├─ /ws       chat (WebSocket)
                          └─ gRPC      sso, chat (grpcurl, grpcui)

sso ──outbox──▶ Kafka users.registered ─┬─▶ notifier (приветственное письмо)
                                        └─▶ chat (копия пользователей)
chat ──Redis pub/sub user:<id>──▶ все реплики chat ──▶ WebSocket получателя
```

| Сервис | Что делает | Хранилища |
|---|---|---|
| sso | регистрация, логин, JWT | Postgres, Redis, Kafka (producer) |
| notifier | реакция на регистрацию | Postgres, Kafka (consumer) |
| chat | чаты, сообщения, WebSocket | Postgres, Redis pub/sub, Kafka (consumer) |
| gateway | REST/JSON для браузера поверх gRPC | — |

## Структура (golang-standards/project-layout)

```
api/proto/           контракты .proto (gRPC, REST-аннотации, события Kafka)
pkg/api/             сгенерированный Go-код (buf generate)
cmd/<service>/       main.go каждого сервиса
internal/<service>/  код сервиса, недоступный снаружи модуля
test/<service>/      функциональные тесты (Testcontainers)
web/                 веб-интерфейс
configs/             примеры env-конфигов для локального запуска
build/package/       Dockerfile'ы
deployments/         docker-compose, nginx, манифесты Kubernetes (kind)
.github/workflows/   CI: тесты, линтеры, сборка образов
```

## Запуск

```bash
cp .env.example .env          # задай пароли и APP_SECRET
make certs                    # самоподписанный сертификат для nginx
make up                       # весь стек в Docker Compose
```

Открой https://localhost:8443, прими предупреждение о сертификате, зарегистрируй двух пользователей в двух окнах и переписывайся.

```bash
make test      # тесты
make lint      # golangci-lint + buf lint
make logs      # логи сервисов
```

Kubernetes (kind):

```bash
make kind-up kind-load
kubectl create namespace messenger
kubectl -n messenger create secret generic sso-db --from-literal=password=...
kubectl -n messenger create secret generic notifier-db --from-literal=password=...
kubectl -n messenger create secret generic chat-db --from-literal=password=...
kubectl -n messenger create secret generic app-jwt --from-literal=secret=...
make k8s-apply
kubectl -n messenger port-forward svc/nginx 8443:8443
```
