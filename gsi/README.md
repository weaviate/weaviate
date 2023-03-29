
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
* cd into the "gsi" directory
* run the docker build script: "./rundockerbuild.sh" which builds the container "gsi/weaviate"
* cd into the "gsi/docker/t2v" directory
* run the docker build script: "./runbuild.sh" which builds the container "gsi/t2v"
* cd into the "gsi/docker" directory
* run the following:  "docker-compose -f docker-compose-sanity.yml up" which runs the containers you just built

Test:
* in another terminal, cd into the "gsi/tests" directory
* create a new python=3.8 environment, activate it, and install the python packages via the "requirements.txt" file
* run the following: "python3 weaviate-sanity.py"
* the example program will create a Gemini index, import some data, and will finally perform some example searches

# Benchmarks

## Gemini Fast Vector Search

The Weaviate Gemini Plugin interfaces directly with GSI's Fast Vector Search (FVS) REST web service.

We benchmarked an FVS system with 4 Gemini APU hardware acceleration boards.  Please see this README for those detailed results.  We've provided code and instructions so you can reproduce these results on your system.

## Gemini Plugin vs Weaviate's Native HNSW

[TBD]

# Troubleshooting

[TBD]
