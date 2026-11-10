FROM golang:1.27-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY pkg ./pkg
COPY cmd/chat ./cmd/chat
COPY internal/chat ./internal/chat
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/chat ./cmd/chat

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/chat /chat

EXPOSE 50052 8081

ENTRYPOINT ["/chat"]