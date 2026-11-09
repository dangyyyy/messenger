# messenger

Микросервисный мессенджер на Go: gRPC, PostgreSQL, Redis, Kafka (transactional outbox), Docker, Kubernetes.

## Сервисы

- **sso**: регистрация, логин, JWT, rate limit в Redis, публикация событий через outbox.
- **notifier**: читает `users.registered` из Kafka, идемпотентная обработка через inbox.

## Структура (golang-standards/project-layout)

```
api/proto/         контракты .proto (gRPC и события Kafka)
pkg/api/           сгенерированный Go-код из proto (buf generate)
cmd/<service>/     main.go каждого сервиса
internal/<service>/  код сервиса, недоступный снаружи модуля
internal/<service>/migrations/  SQL-миграции (goose, embed)
test/<service>/    функциональные тесты (Testcontainers)
configs/           примеры env-конфигов для локального запуска
build/package/     Dockerfile'ы
deployments/       docker-compose и манифесты Kubernetes (kind)
.github/workflows/ CI: тесты, линтеры, сборка образов
```

## Запуск

```bash
cp .env.example .env
cp configs/sso.env.example configs/sso.env
cp configs/notifier.env.example configs/notifier.env

make up        # весь стек в Docker Compose
make test      # тесты
make lint      # golangci-lint + buf lint
```

Kubernetes:

```bash
make kind-up kind-load
# создать секреты sso-db и notifier-db (kubectl create secret ...)
make k8s-apply
```
