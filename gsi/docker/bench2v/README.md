
Custom T2V (Transformer-based Vectorization) Containers

# Build

To build a t2v compatible container locally, edit (as needed) and run the file run_build.sh.

By default the script is trying to embed 'distilroberta' and will write a container called 'gsi-t2v' but you can change those settings.

You should keep the defaults unless you really need to change them.

# Test

Edit (as needed) and run the docker-compose.yml file in this directory.

Run the smoke_test.py file in this directory to test your container.

# Troubleshooting

We took several files from the original Weaviate repo at https://github.com/weaviate/t2v-transformers-models, so maybe you can start there if you are having problems.

One issue we can into was regarding the docker.io login.  We followed the directions to setup docker credentials here:  https://github.com/docker/docker-credential-helpers/issues/102


