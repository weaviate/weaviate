# OpenTelemetry Integration with Weaviate

This directory contains the OpenTelemetry configuration for distributed tracing in Weaviate.

## Quick Start

1. **Start the development environment with Tempo:**
   ```bash
   ./tools/dev/restart_dev_environment.sh --prometheus --tempo
   ```

2. **Start Weaviate with OpenTelemetry enabled:**
   ```bash
   ./tools/dev/run_dev_server.sh local-single-node-otel
   ```

## Alternative: Using Docker Compose Directly

You can also start just the monitoring services:

```bash
docker-compose up -d prometheus grafana tempo
```

## Services

- **Tempo**: Distributed tracing backend (http://localhost:3200)
- **Prometheus**: Metrics collection (http://localhost:9090)
- **Grafana**: Visualization dashboard (http://localhost:3000, admin/admin)
- **OTLP gRPC**: localhost:4317
- **OTLP HTTP**: localhost:4318

## Configuration Files

- `tempo.yaml`: Tempo configuration for trace collection and storage
- `tools/dev/grafana/datasources/tempo.yml`: Grafana Tempo datasource
- `tools/dev/prometheus_config/prometheus.yml`: Prometheus configuration with Tempo metrics

## Viewing Traces

1. Open Grafana at http://localhost:3000 (admin/admin)
2. Go to Explore
3. Select "Tempo" as the data source
4. Query traces by service name, trace ID, or other attributes

## Environment Variables

When using `local-single-node-otel`, these OpenTelemetry variables are automatically set:

- `OTEL_ENABLED=true`
- `OTEL_EXPORTER_OTLP_ENDPOINT=localhost:4317`
- `OTEL_EXPORTER_OTLP_PROTOCOL=grpc`
- `OTEL_TRACES_SAMPLER_ARG=1.0`

## Stopping Services

```bash
# Stop monitoring stack
docker stop prometheus grafana tempo

# Stop Weaviate (Ctrl+C if running in foreground)
```

## Integration with Development Environment

The Tempo service is integrated into the main development environment:

- **Using restart script**: `./tools/dev/restart_dev_environment.sh --prometheus --tempo`
- **Using Docker Compose directly**: `docker-compose up -d tempo prometheus grafana`

This allows you to use the existing development infrastructure with OpenTelemetry tracing. 