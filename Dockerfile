# One image build, parameterised by module.
#
# Every module in this repo shares a go.mod, so the dependency layer caches
# across all eighteen builds — the first is slow, the rest reuse it. MODULE
# picks which main gets compiled; the resulting image is still one module, with
# its own chart, ServiceAccount and RBAC. Merging the BINARIES would mean one
# ServiceAccount holding the union of every module's permissions, which is why
# this parameterises the build rather than the runtime.
#
# Build on the NATIVE runner arch (BUILDPLATFORM) and let Go cross-compile —
# fast, no QEMU. Only the tiny final stage is per-target.
FROM --platform=$BUILDPLATFORM golang:1.25 AS builder
ARG TARGETOS
ARG TARGETARCH
ARG VERSION
ARG MODULE
WORKDIR /src
COPY go.mod go.sum ./
RUN go mod download
COPY . .
RUN test -n "$MODULE" || (echo "MODULE build-arg is required" >&2; exit 1)
RUN CGO_ENABLED=0 GOOS=${TARGETOS:-linux} GOARCH=${TARGETARCH:-amd64} \
    go build -ldflags="-X github.com/tiny-systems/module/cli.versionID=${VERSION}" \
    -o /bin/manager ./${MODULE}/cmd

FROM gcr.io/distroless/static:nonroot
# GHCR links a package to a repository via this label, and that link is what
# decides which workflow tokens may push. Without it these packages stay bound
# to the eighteen repositories they were built from, and a token issued to this
# one is denied — which is exactly how the first release failed.
ARG MODULE
LABEL org.opencontainers.image.source="https://github.com/tiny-systems/modules"
LABEL org.opencontainers.image.description="tiny-systems ${MODULE}"
LABEL org.opencontainers.image.licenses="Apache-2.0"
COPY --from=builder /bin/manager /manager
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
USER 65532:65532
CMD ["/manager", "run"]
