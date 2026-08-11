# syntax=docker/dockerfile:1

FROM golang:1.23-alpine AS build
WORKDIR /src

# Dependencies first so code edits don't invalidate the module cache.
COPY go.mod go.sum ./
RUN go mod download

COPY . .
ARG TARGETOS TARGETARCH
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH} \
    go build -trimpath -ldflags="-s -w" -o /out/copypasta .

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/copypasta /copypasta
EXPOSE 8080
USER nonroot:nonroot
ENTRYPOINT ["/copypasta"]
