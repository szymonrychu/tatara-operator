# syntax=docker/dockerfile:1.7

ARG GO_VERSION=1.26
FROM golang:${GO_VERSION}-alpine AS builder

WORKDIR /src
RUN apk add --no-cache git ca-certificates

COPY go.mod go.sum ./
RUN go mod download

COPY . .

RUN mkdir -p /out

ARG VERSION=dev
ARG COMMIT=unknown
ARG DATE=unknown

RUN CGO_ENABLED=0 GOOS=linux go build \
    -trimpath \
    -ldflags "-s -w \
      -X github.com/szymonrychu/tatara-operator/internal/version.Version=${VERSION} \
      -X github.com/szymonrychu/tatara-operator/internal/version.Commit=${COMMIT} \
      -X github.com/szymonrychu/tatara-operator/internal/version.Date=${DATE}" \
    -o /out/tatara-operator \
    ./cmd/manager

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=builder /out/tatara-operator /tatara-operator
USER nonroot:nonroot
EXPOSE 8080 8081 9090
ENTRYPOINT ["/tatara-operator"]
