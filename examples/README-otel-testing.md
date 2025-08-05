# OpenTelemetry Testing Environment

This directory contains everything you need to test the OpenTelemetry integration with Weaviate locally.

## Quick Start

1. **Start the testing environment:**
   ```bash
   cd examples
   ./start-otel-testing.sh
   ```

2. **Configure Weaviate to send traces:**
   ```bash
   export OTEL_ENABLED=true
   export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317
   export OTEL_EXPORTER_OTLP_PROTOCOL=grpc
   export OTEL_TRACES_SAMPLER_ARG=1.0
   ```

3. **Start Weaviate with tracing enabled:**
   ```bash
   # Your normal Weaviate startup command
   go run cmd/weaviate-server/main.go
   ```

4. **View traces in the UI:**
   - Jaeger: http://localhost:16686
   - Zipkin: http://localhost:9411

## What's Included

### Services

- **OpenTelemetry Collector** - Receives traces from Weaviate and forwards them to backends
- **Jaeger** - Distributed tracing backend with web UI
- **Zipkin** - Alternative distributed tracing backend
- **Prometheus** - Metrics collection (optional)
- **Grafana** - Metrics visualization (optional)

### Configuration Files

- `otel-collector-config.yaml` - OpenTelemetry Collector configuration
- `docker-compose-otel-testing.yml` - Docker Compose setup
- `prometheus.yml` - Prometheus configuration
- `start-otel-testing.sh` - Startup script

## Manual Setup

If you prefer to start services manually:

```bash
# Start the services
docker-compose -f docker-compose-otel-testing.yml up -d

# Check status
docker-compose -f docker-compose-otel-testing.yml ps

# View logs
docker-compose -f docker-compose-otel-testing.yml logs -f

# Stop services
docker-compose -f docker-compose-otel-testing.yml down
```

## Testing Different Configurations

### Test with HTTP Exporter
```bash
export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4318
export OTEL_EXPORTER_OTLP_PROTOCOL=http
```

### Test with Different Sampling Rates
```bash
# 10% sampling (recommended for production)
export OTEL_TRACES_SAMPLER_ARG=0.1

# 100% sampling (for debugging)
export OTEL_TRACES_SAMPLER_ARG=1.0

# No sampling
export OTEL_TRACES_SAMPLER_ARG=0.0
```

### Test with Custom Service Name
```bash
export OTEL_SERVICE_NAME=weaviate-test
export OTEL_ENVIRONMENT=development
```

## Troubleshooting

### Services Not Starting
```bash
# Check Docker is running
docker info

# Check available ports
netstat -an | grep -E "(4317|4318|16686|9411)"

# View detailed logs
docker-compose -f docker-compose-otel-testing.yml logs
```

### Traces Not Appearing
1. Verify Weaviate is configured correctly:
   ```bash
   echo $OTEL_ENABLED
   echo $OTEL_EXPORTER_OTLP_ENDPOINT
   ```

2. Check collector logs:
   ```bash
   docker-compose -f docker-compose-otel-testing.yml logs otel-collector
   ```

3. Verify network connectivity:
   ```bash
   curl http://localhost:4318/health
   ```

### Performance Issues
- Reduce sampling rate: `export OTEL_TRACES_SAMPLER_ARG=0.1`
- Increase batch timeout: `export OTEL_BSP_EXPORT_TIMEOUT=10s`
- Reduce batch size: `export OTEL_BSP_MAX_EXPORT_BATCH_SIZE=256`

## Collector Configuration Details

The OpenTelemetry Collector is configured to:

- **Receive** OTLP traces via gRPC (port 4317) and HTTP (port 4318)
- **Process** traces with batching, memory limiting, and resource attribution
- **Export** traces to:
  - Console logging (for debugging)
  - Jaeger (for visualization)
  - Zipkin (alternative visualization)

### Customizing the Collector

Edit `otel-collector-config.yaml` to:
- Add more exporters (e.g., to cloud providers)
- Modify processing pipelines
- Change sampling strategies
- Add custom attributes

## Integration with Cloud Providers

To send traces to cloud providers, modify the collector configuration:

### Google Cloud Trace
```yaml
exporters:
  googlecloud:
    project: your-project-id
    credentials_file: /path/to/service-account.json
```

### AWS X-Ray
```yaml
exporters:
  awsxray:
    region: us-east-1
    resource_arn: arn:aws:ecs:us-east-1:123456789012:cluster/your-cluster
```

### Azure Application Insights
```yaml
exporters:
  azuremonitor:
    connection_string: "InstrumentationKey=your-key"
```

## Cleanup

To completely remove the testing environment:

```bash
# Stop and remove containers
docker-compose -f docker-compose-otel-testing.yml down -v

# Remove images (optional)
docker rmi otel/opentelemetry-collector:latest
docker rmi jaegertracing/all-in-one:latest
docker rmi openzipkin/zipkin:latest
```

## Next Steps

Once you've verified the basic OpenTelemetry integration works:

1. **Test with real Weaviate operations** - Create objects, perform searches, etc.
2. **Monitor performance impact** - Check CPU and memory usage
3. **Configure production settings** - Adjust sampling rates and batch sizes
4. **Integrate with your observability stack** - Send traces to your preferred backend 