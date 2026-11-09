FROM golang:1.27-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY gen ./gen
COPY services/notifier ./services/notifier
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/notifier ./services/notifier/cmd/notifier

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/notifier /notifier

ENTRYPOINT ["/notifier"]