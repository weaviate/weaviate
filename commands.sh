rm -rf data-weaviate-0 && rm -rf backups-weaviate-0

ASYNC_INDEXING=true ASYNC_INDEXING_BATCH_SIZE=990000 GRPC_MAX_MESSAGE_SIZE=20446800000 QUEUE_SCHEDULER_INTERVAL=60s ./tools/dev/run_dev_server.sh local-cuvs-development

ASYNC_INDEXING=true ASYNC_INDEXING_BATCH_SIZE=8741823 GRPC_MAX_MESSAGE_SIZE=20446800000 QUEUE_SCHEDULER_INTERVAL=120s ./tools/dev/run_dev_server.sh local-cuvs-development 



// for testing client
ASYNC_INDEXING=true ASYNC_INDEXING_BATCH_SIZE=32 ./tools/dev/run_dev_server.sh local-cuvs-development
