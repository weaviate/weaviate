
Weaviate Gemini Plugin

# Introduction

The Weaviate Gemini Plugin provides an alternate ANN implementation for Weaviate based on hardware acceleration powered by GSI Technology's Gemini APU.

The features of this plugin include:
* provides accurate and fast vector search for billion-scale vector datasets 
* the plugin is implemented in pure Golang
* it interfaces with Gemini's FVS (Fast Vector Search) REST web interface

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

# Gemini Configuration

# Benchmarks

## Gemini Fast Vector Search

The Weaviate Gemini Plugin interfaces directly with GSI's Fast Vector Search (FVS) REST web service.

We benchmarked an FVS system with 4 Gemini APU hardware acceleration boards.  Please see this README for those detailed results.  We've provided code and instructions so you can reproduce these results on your system.

## Gemini Plugin vs Weaviate's Native HNSW

[TBD]

# Troubleshooting

[TBD]
