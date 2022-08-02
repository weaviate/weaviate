# weaviate-chaos-engineering <img alt='Weaviate logo' src='https://raw.githubusercontent.com/semi-technologies/weaviate/19de0956c69b66c5552447e84d016f4fe29d12c9/docs/assets/weaviate-logo.png' width='180' align='right' />

> Chaos-Engineering-Style pipelines to make sure Weaviate behaves correctly,
> even when we try to sabotage it a bit.

## What this does

Currently there are two independent pipelines:

### Pipeline 1: Importing while Crashing

This pipelines is a Chaos-Monkey-style pipeline where we aim to import a large
dataset, but an external source constantly introduces crashes. The goal is to
make sure that every crash is recoverable and after the importer is through all
objects were indeed imported.

### Pipeline 2: Compare Recall after restarts

This pipelines imports a specific dataset (which will be generated as part of
the CI) and meassures the recall on it. Then Weaviate is stopped and restarted
with all state coming from disk and the recall is measured again. This pipeline
makes sure that all async operations we perform on commit logs (combining,
condensing, etc.) do not degrade the quality of the vector index.

## How to Run

### Requirements

- Bash
- Docker / Docker Compose
- `jq`

### Run the scripts

Everything is entirely containerized and all pipelines can be started with
simple bash scripts. You can find the scripts in the root folder, such as
`./import_while_crashing.sh` and `./compare_recall_after_restart.sh`. Or simply
check the travis file for an example. 

## Links 

- [Weaviate Main Repo](https://github.com/semi-technologies/weaviate).
- [Documentation](https://weaviate.io/developers/weaviate/current/client-libraries/javascript.html).
- [Stackoverflow for questions](https://stackoverflow.com/questions/tagged/weaviate).
- [Github for issues](https://github.com/semi-technologies/weaviate/issues).
