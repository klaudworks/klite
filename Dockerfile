# Build stage
FROM golang:1.25-bookworm AS build

WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG VERSION=dev
RUN CGO_ENABLED=0 go build -ldflags "-s -w -X main.version=${VERSION}" -o /klite ./cmd/klite

# Runtime stage
FROM gcr.io/distroless/static-debian12

COPY --from=build /klite /klite

EXPOSE 9092
VOLUME /data

ENTRYPOINT ["/klite"]
CMD ["--data-dir", "/data"]
