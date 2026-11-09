FROM golang:1.27-alpine AS build

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY gen ./gen
COPY services/sso ./services/sso
RUN CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/sso ./services/sso/cmd/sso

FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=build /out/sso /sso

EXPOSE 50051

ENTRYPOINT ["/sso"]