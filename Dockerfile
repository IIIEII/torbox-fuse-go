# ---- Builder ----
FROM golang:1.26-alpine AS builder

RUN apk add --no-cache gcc musl-dev

WORKDIR /src

COPY go.mod go.sum ./
RUN go mod download

COPY . .
RUN CGO_ENABLED=1 go build -ldflags="-s -w" -o /torbox-media-center ./cmd/torbox-media-center

# ---- Runtime ----
FROM alpine:3.21

RUN apk add --no-cache fuse3 ca-certificates

COPY --from=builder /torbox-media-center /usr/local/bin/torbox-media-center

ENTRYPOINT ["torbox-media-center"]