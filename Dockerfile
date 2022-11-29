# Build
FROM golang:1.17.0-alpine AS builder

ENV GO111MODULE=on
ENV GOPATH=/

COPY . .

COPY DigiCertGlobalRootCA.crt.pem /
RUN CGO_ENABLED=0 GOOS=linux go build -ldflags="-s -w" -trimpath -o /main cmd/main.go

# Deploy
FROM alpine

# Copy main and DigiCertGlobalRootCA.crt.pem from builder
COPY --from=builder main .
COPY --from=builder DigiCertGlobalRootCA.crt.pem .

EXPOSE 9090

CMD ["./main"]