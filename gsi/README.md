
Weaviate Gemini Plugin

# Introduction

The Weaviate Gemini Plugin provides an alternate ANN implementation for Weaviate based on hardware acceleration powered by GSI Technology's Gemini APU.

NOTE: This is an internal 'alpha' version of the plugin development and is subject to change if/when it gets merged into the main Weaviate repo.

# Installation

## Gemini-enabled Weaviate Container

Currently, Gemini-enabled Weaviate is provided by a custom Weaviate container.  That container can replace an original Weaviate container in your docker-compose files.

To build Gemini-enabled Weaviate container, following these directions:

* login to a machine with docker and Gemini FVS installed
* clone this repository and cd to the top-level directory.  if you already have it cloned, make sure to pull the latest master branch and make sure you are in master locally.
* run the script './rundockerbuild.sh
* if successful, it will create a container called 'gsi/weaviate'
* if it's not sucessful, see the troubleshooting section below for possible resolutions

Now, before you replace this Gemini-enabled Weaviate container in your application, please run the sanity check below.

## Test the Gemini-enabled Weaviate Container

* cd into 'gsi/docker/t2v' and follow the README instructions in there to build a custom t2v (transformer-2-vec) vectorizer container called 'gsi-t2v.'
* cd into 'gsi/docker' and edit the file 'docker-compose-sanity.yml' with a valid allocation_id
* run 'docker-compose -f docker-compose-sanity.yml up'.  This will start the both the Gemini-enabled Weaviate container and the custom 't2v' container you just built in the previous steps.
* in another terminal, cd into 'gsi/tests'.  Activate a new python 3.8 environment with the requirements.txt file in there.  Then run the example program called 'weaviate-sanity.py.'  The script attempts to add a bunch of documents to Weaviate (which invokes 't2v' for vectorization), and then it waits for the Gemini index to build the index, and then it performs finally a nearest neighbor searches.

## Troubleshooting

[TBD]

# Benchmarks

Currently, we only have FVS benchmarks, on top of which the Gemini Plugin is built.  See the 'fvs' sub-directory for more information.

