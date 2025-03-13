#!/bin/sh

# Start Ollama in background
ollama serve &
OLLAMA_PID=$!

# Wait for Ollama to start up
echo "Waiting for Ollama service to be ready..."
until ollama list > /dev/null 2>&1; do
  echo "Waiting for Ollama service to be ready..."
  sleep 2
done
echo "Ollama is running!"

# Pull required models
MODELS=${MODELS_TO_PULL:-llama3}
echo "$MODELS" | tr ',' '\n' | while read MODEL; do
  if [ -n "$MODEL" ]; then
    echo "Pulling model: $MODEL"
    ollama pull "$MODEL"
  fi
done

echo "All models loaded successfully!"

# Keep container running
wait $OLLAMA_PID
