
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
* You should have received already an "allocation id" (which behaves like an API key.)
* Without all of these you should not proceed and should reach out to your GSI Technology support contact.

Container Setup:
* If you haven't already, clone this repository on the system and cd into the top-level directory.
* Now cd into the "gsi" directory
* Run the docker build script "rundockerbuild.sh" which builds the Gemini plugin-enabled container locally called "gsi/weaviate"
* Now cd into the "gsi/docker/t2v" directory
* Run the docker build script "rundockerbuild.sh" which builds a vectorization container locally called "gsi/t2v"
* Now cd into the "gsi/docker" directory
* Run the docker-compose file "docker-compose-sanity.yml," which runs the containers you just built.
* Wait until you see the startup message wihich indicates that Weaviate has successfully start.

Test Program:
* In another terminal, cd into the "gsi/tests" directory
* Create a new python=3.8 environment, activate it, and install the python packages via the "requirements.txt" file
* Run the following python file "weaviate-gemini-sanity.py"
* This weaviate python program will create a gemini index, import some data into it, and will finally perform some example searches against the index.

# Gemini Plugin Configuration in Weaviate

The Gemini plugin requires minimal configuration at the server and the client. 

# Weaviate Server Gemini Plugin Configuration

This codebase contains all the Gemini Plugin code, so build and deploy the container as you would normally (see the script at "gsi/rundockerbuild.sh." )

The following environmental variables are unique to Gemini Plugin configuration at the server:
* GEMINI_ALLOCATION_ID - This must be set to a valid allocation id.  Please consult your onboarding instructions or reach out to your GSI support contact for more information.
* GEMINI_DATA_DIRECTORY - This directory must exist and live under /home/public ( a requirement of the FVS component. )
* GEMINI_FVS_SERVER - This is the server address of your FVS server.  Typically, its value should be 'localhost' since the FVS server would typically be co-located with your Weaviate server instance.
* GEMINI_DEBUG - By default the value is 'false'.  Set to 'true' to see more Gemini Plugin log messages printed to the console.

See the docker-compose file at 'gsi/docker/docker-compose-sanity.yml' for an example of how to set these environmental variables.

# Weaviate Client Configuration

The default vector index is Weaviate's native implementation of HNSW.  To override this in your application, you must configure the 'vectorIndexType' property when you create your Weaviate class.

There is a python example at "gsi/tests/weaviate-gemini-sanity.py" that demonstrates how to do this.

# Benchmarks

## Gemini Fast Vector Search

The Weaviate Gemini Plugin communicates with GSI's Fast Vector Search (FVS) REST web service.

We benchmarked a system with 4 Gemini APU hardware acceleration boards.  Please see this [README](fvs/README.md) for those detailed results.  In that directory, you will find code and instructions so that you can reproduce these results on your system.

## Gemini Plugin vs Weaviate's Native HNSW

[These benchmarks are not yet available.]

# Limitations

## Index Training

The algorithm that powers the Gemini Plugin (via the FVS) requires an index training/build step.  This contrasts to the native HNSW algorithm which builds its index incrementally and dynamically as the application adds vectors. The Gemini Plugin launches its index training operation when the application invokes its first "search" operation.  The training operation subsequently runs asynchronously and in the background, and therefore does not block the application.  The application immediately receives a message that indicates that the asynchronous training operation has started.  Weaviate developers should take note of this and should consider modifying their application's control flow accordingly when using the Gemini Plugin.

Ideally, an application that leverages the Gemini Plugin should be structured as follows:
* add all the objects that need to be vectorized first via the relevant Weaviate API calls.
* perform a Weaviate "search" API call and look for a response message that indicates "asynchronous index training in progress."
* continue querying Weaviate until the response message indicates that the "asynchrnous index training is complete."
* subsequent search calls will complete as you would normally expect.

We chose this route because it does not require you to install a new client side library for your application.

As datasets become larger, the elapsed time that your application needs to wait for the index training to complete will increase as well.  Please see the (Gemini training benchmarks)[fvs/README.md] for more information.

Also, note the following current limitations while the Gemini index is training:
* Additional object/vector adds are not allowed
* Delete object/vectors are not allowed

Also currently the Gemin Plugin will train a Gemini index once during its lifetime.

## CI/CD Automation

Test automation that extends the existing Weaviate CI/CD on systems with the FVS and APU boards is not yet available.

## Replication

High-availability configuration and support like replication are not yet available.

# Troubleshooting

[This section will contain resolutions to common problems that you may run into when either configuring the Gemini Plugin, or working with a Gemini index, the FVS system, or the APU hardware.]


