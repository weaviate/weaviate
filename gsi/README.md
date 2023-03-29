
# Gemini Weaviate Plugin

The Weaviate Gemini Plugin provides an alternate ANN implementation for Weaviate based on hardware acceleration powered by GSI Technology's Gemini APU.

The features of this plugin include:
* provides accurate and fast vector search for large-scale vector datasets 
* the plugin is implemented in pure Golang
* the interfaces with Gemini's FVS (Fast Vector Search) REST web interface

# Architecture

The following is a high-level architecture of a system with the Weaviate Gemini Plugin:

[TBD]

# Quick Start

If you just want to see something working first, then following the directions in this section.

Prerequisites:
* You should have access to a system with Gemini APU boards installed.
* The system should have GSI's FVS (Fast Vector Search) support installed.
* You should have received already an ALLOCATION_ID (which behaves like an API key.)
* Without all of these you should not proceed and should reach out to your GSI Technology support contact.

Container Setup:
* If you haven't already, clone this repository on the system and cd into the top-level directory.
* Now cd into the "gsi" directory
* Run the docker build script "rundockerbuild.sh" which builds the Gemini plugin-enabled container locally called "gsi/weaviate"
* Now cd into the "gsi/docker/t2v" directory
* Run the docker build script "rundockerbuild.sh" which builds a vectorization container locally called "gsi/t2v"
* Now cd into the "gsi/docker" directory
* Run the following docker-compose file "docker-compose-sanity.yml up" which runs the containers you just built.

Test Program:
* In another terminal, cd into the "gsi/tests" directory
* Create a new python=3.8 environment, activate it, and install the python packages via the "requirements.txt" file
* Run the following python file "weaviate-sanity.py"
* This weaviate python program will create a gemini index, import some data into it, and will finally perform some example searches against the index.

# Gemini Support In Weaviate

The Gemini plugin requires minimal configuration at the server and the client. 

# Weaviate Server Gemini Plugin Configuration

This codebase contains the Gemini Plugin, so build and deploy the container as you would normally.  See the script at "gsi/rundockerbuild.sh" for an example.

The following environmental variables are unique to Gemini:
* GEMINI_ALLOCATION_ID - This must be set to a valid allocation id.  Please consult your onboarding instructions or reach out to your GSI support contact.
* GEMINI_DATA_DIRECTORY - This directory must exist and live under /home/public ( a requirement of the FVS component. )
* GEMINI_FVS_SERVER - This is the server address of your FVS server.  Typically, its value should be 'localhost' since the FVS server is co-located with your Weaviate eerver instance.
* GEMINI_DEBUG - By default the value is 'false'.  Set to 'true' to see more logging messages to the console.

See the docker-compose file at 'gsi/docker/docker-compose-sanity.yml' for an example of how to set these environmental variables.

# Weaviate Client Configuraation

The default vector index is Weaviate's native implementation of HNSW.  To override this in your application, you must configure the 'vectorIndexType' property when you create your Weaviate class.

There is a python example at "gsi/tests/weaviate-gemini-sanity.py" that demonstrates how to do this.

# Benchmarks

## Gemini Fast Vector Search

The Weaviate Gemini Plugin interfaces directly with GSI's Fast Vector Search (FVS) REST web service.

We benchmarked an FVS system with 4 Gemini APU hardware acceleration boards.  Please see this README for those detailed results.  We've provided code and instructions so you can reproduce these results on your system.

## Gemini Plugin vs Weaviate's Native HNSW

[TBD]

# Troubleshooting

[TBD]
