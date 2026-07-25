# --- Stage 1: build the dashboard ---
FROM node:22-alpine AS web
WORKDIR /web
COPY web/package.json web/package-lock.json ./
RUN npm ci
COPY web/ ./
RUN npm run build

# --- Stage 2: build the Go server, embedding the dashboard build ---
FROM golang:1.25-alpine AS go-build
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
COPY --from=web /web/dist ./web/dist
RUN CGO_ENABLED=0 go build -o /out/aegisd ./cmd/aegisd
RUN CGO_ENABLED=0 go build -o /out/aegis ./cmd/aegis

# --- Stage 3: minimal runtime image ---
FROM alpine:3.20
RUN adduser -D -u 10001 aegis
COPY --from=go-build /out/aegisd /usr/local/bin/aegisd
COPY --from=go-build /out/aegis /usr/local/bin/aegis
USER aegis
WORKDIR /data
EXPOSE 8080
ENTRYPOINT ["aegisd"]
