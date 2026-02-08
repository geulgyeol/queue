# Build stage
FROM --platform=$BUILDPLATFORM golang:1.24.3-alpine AS builder

# Install build dependencies
RUN apk add --no-cache git

# Set working directory
WORKDIR /app

# Copy go mod files
COPY go.mod go.sum ./

# Download dependencies
RUN go mod download

# Copy source code
COPY . .

RUN go install github.com/sqlc-dev/sqlc/cmd/sqlc@latest
RUN sqlc generate

ARG TARGETOS=linux
ARG TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS} GOARCH=${TARGETARCH} go build -a -installsuffix cgo -o geulgyeol-queue .

# Final stage
FROM --platform=$TARGETPLATFORM alpine:latest


# Create data directory
RUN mkdir -p /data

WORKDIR /root

# Copy the binary from builder
COPY --from=builder /app/geulgyeol-queue .

# Expose the default port
EXPOSE 8080

# Run the application
ENTRYPOINT ["./geulgyeol-queue"]