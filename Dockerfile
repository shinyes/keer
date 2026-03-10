FROM --platform=$BUILDPLATFORM golang:1.25-alpine AS builder

WORKDIR /src

RUN apk add --no-cache ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -trimpath -ldflags="-s -w" -o /out/keer-server ./cmd/server

FROM alpine:3.21

RUN apk add --no-cache ca-certificates tzdata && \
    addgroup -S keer && \
    adduser -S -G keer keer

WORKDIR /app

COPY --from=builder /out/keer-server /usr/local/bin/keer-server

RUN mkdir -p /app/data/uploads && \
    chown -R keer:keer /app

ENV APP_ADDR=:12843 \
    BASE_URL=http://localhost:12843 \
    DB_PATH=/app/data/keer.db \
    UPLOADS_DIR=/app/data/uploads

EXPOSE 12843
VOLUME ["/app/data"]

USER keer

ENTRYPOINT ["/usr/local/bin/keer-server"]
