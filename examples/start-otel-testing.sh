#!/bin/bash

# OpenTelemetry Testing Environment Startup Script
# This script starts the OpenTelemetry Collector, Jaeger, and Zipkin for local testing

set -e

echo "🚀 Starting OpenTelemetry Testing Environment..."

# Check if Docker is running
if ! docker info > /dev/null 2>&1; then
    echo "❌ Docker is not running. Please start Docker and try again."
    exit 1
fi

# Check if docker-compose is available
if ! command -v docker-compose &> /dev/null; then
    echo "❌ docker-compose is not installed. Please install docker-compose and try again."
    exit 1
fi

# Get the directory where this script is located
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Change to the examples directory
cd "$SCRIPT_DIR"

echo "📁 Working directory: $(pwd)"

# Check if required files exist
if [ ! -f "otel-collector-config.yaml" ]; then
    echo "❌ otel-collector-config.yaml not found in $(pwd)"
    exit 1
fi

if [ ! -f "docker-compose-otel-testing.yml" ]; then
    echo "❌ docker-compose-otel-testing.yml not found in $(pwd)"
    exit 1
fi

# Stop any existing containers
echo "🛑 Stopping any existing containers..."
docker-compose -f docker-compose-otel-testing.yml down --remove-orphans

# Start the services
echo "🔧 Starting OpenTelemetry Collector, Jaeger, and Zipkin..."
docker-compose -f docker-compose-otel-testing.yml up -d

# Wait for services to be ready
echo "⏳ Waiting for services to be ready..."
sleep 10

# Check if services are running
echo "🔍 Checking service status..."
docker-compose -f docker-compose-otel-testing.yml ps

echo ""
echo "✅ OpenTelemetry Testing Environment is ready!"
echo ""
echo "📊 Available UIs:"
echo "   • Jaeger UI: http://localhost:16686"
echo "   • Zipkin UI: http://localhost:9411"
echo "   • Prometheus: http://localhost:9090"
echo "   • Grafana: http://localhost:3000 (admin/admin)"
echo ""
echo "🔌 Collector Endpoints:"
echo "   • OTLP gRPC: localhost:4317"
echo "   • OTLP HTTP: localhost:4318"
echo ""
echo "🔧 To configure Weaviate to send traces to this collector, set:"
echo "   export OTEL_ENABLED=true"
echo "   export OTEL_EXPORTER_OTLP_ENDPOINT=http://localhost:4317"
echo "   export OTEL_EXPORTER_OTLP_PROTOCOL=grpc"
echo "   export OTEL_TRACES_SAMPLER_ARG=1.0"
echo ""
echo "🛑 To stop the environment, run:"
echo "   docker-compose -f docker-compose-otel-testing.yml down"
echo ""
echo "📝 To view logs, run:"
echo "   docker-compose -f docker-compose-otel-testing.yml logs -f" 