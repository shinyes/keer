# syntax=docker/dockerfile:1.7

FROM --platform=$BUILDPLATFORM golang:1.25.5-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .

ARG TARGETOS
ARG TARGETARCH

RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/keer ./cmd/server

FROM gcr.io/distroless/base-debian12

WORKDIR /app

COPY --from=build /out/keer /app/keer

ENV APP_ADDR=:12843
ENV DB_PATH=/data/keer.db
ENV UPLOADS_DIR=/data/uploads

EXPOSE 12843
VOLUME ["/data"]

ENTRYPOINT ["/app/keer"]
