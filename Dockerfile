# syntax=docker/dockerfile:1.7
FROM golang:1.25-alpine AS build
ARG SERVICE
WORKDIR /src
COPY go.mod go.sum ./
RUN --mount=type=cache,target=/go/pkg/mod go mod download
COPY . .
RUN --mount=type=cache,target=/root/.cache/go-build \
    test -n "${SERVICE}" \
    && CGO_ENABLED=0 go build -trimpath -ldflags="-s -w" -o /out/bastion "./services/${SERVICE}/cmd"

FROM gcr.io/distroless/static-debian12:nonroot
COPY --from=build /out/bastion /bastion
USER nonroot:nonroot
ENTRYPOINT ["/bastion"]
