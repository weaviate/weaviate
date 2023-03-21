
# Build

To build a t2v compatible container locally, edit and then run the file run_build.sh.

By default the script is trying to embed 'distilroberta' and will write a container called 'gsi-t2v' but you can change those settings.

# Troubleshooting

We took several files from the original Weaviate repo at https://github.com/weaviate/t2v-transformers-models, so maybe you can start there if you are having problems.

One issue we can into was regarding the docker.io login.  We followed the directions to setup docker credentials here:  https://github.com/docker/docker-credential-helpers/issues/102


