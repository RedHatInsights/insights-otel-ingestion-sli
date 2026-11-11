ARG GO_IMAGE=registry.access.redhat.com/ubi10/go-toolset:1.26
ARG RUNTIME_IMAGE=registry.access.redhat.com/ubi10/ubi-minimal:latest

FROM ${GO_IMAGE} AS build
USER 1001
WORKDIR /opt/app-root/src
COPY --chown=1001:0 go.mod go.sum ./
RUN go mod download
COPY --chown=1001:0 . .
RUN CGO_ENABLED=0 go build -mod=readonly -trimpath -ldflags='-s -w' -o /tmp/collector ./cmd/collector

FROM ${RUNTIME_IMAGE}
USER 0
RUN microdnf install -y ca-certificates && microdnf clean all \
    && mkdir -p /var/lib/otelcol /etc/otelcol \
    && chgrp -R 0 /var/lib/otelcol /etc/otelcol \
    && chmod -R g=u /var/lib/otelcol /etc/otelcol
COPY --from=build --chmod=0755 /tmp/collector /usr/local/bin/insights-otel-ingestion-sli
COPY --chmod=0644 configs/collector.yaml /etc/otelcol/config.yaml
COPY --chmod=0644 LICENSE NOTICE /licenses/
USER 1001:0
EXPOSE 4317 4318 8889 13133
ENTRYPOINT ["/usr/local/bin/insights-otel-ingestion-sli"]
CMD ["--config", "/etc/otelcol/config.yaml"]
