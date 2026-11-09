FROM golang:1.27-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY pkg ./pkg
COPY cmd/notifier ./cmd/notifier
COPY internal/notifier ./internal/notifier
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/notifier ./cmd/notifier

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/notifier /notifier

ENTRYPOINT ["/notifier"]