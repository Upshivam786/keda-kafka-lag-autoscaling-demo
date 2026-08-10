FROM golang:1.25-alpine AS builder

WORKDIR /src

COPY app/go.mod app/go.sum ./app/
WORKDIR /src/app

RUN go mod download

COPY app/ ./

RUN CGO_ENABLED=0 GOOS=linux GOARCH=amd64 \
    go build -trimpath -ldflags="-s -w" \
    -o /out/consumer ./cmd/consumer


FROM gcr.io/distroless/static-debian12:nonroot

COPY --from=builder /out/consumer /consumer

EXPOSE 8080

USER nonroot:nonroot

ENTRYPOINT ["/consumer"]
