#!/usr/bin/env python3

##
# USAGE: input with the config file to update and the name of the config.
# Example: `$ ./tools/set_latest_contextionary.py ./tools/dev/config.json janusgraph_docker`
##

import os
import sys
import json
import requests

##
# Set variables
##
inputJson = sys.argv[1]
configToEdit = sys.argv[2]

##
# Validate if path is set
##
if os.path.isfile(inputJson) is False:
    print("The input file is not a file or not available. Input = " + inputJson)
    exit(1)

##
# Validate if the config is available
##
foundEnvironment = False

with open(inputJson) as f:
    data = json.load(f)

##
# Set the newdata file
##
newData = {}
newData["environments"] = []

for environment in data["environments"]:
    if environment["name"] == configToEdit:
        foundEnvironment = True

if foundEnvironment == False:
    print("Can't find the Weaviate environment: " + configToEdit)
    exit(1)

##
# Load latest contextionary version
##
r = requests.get("https://contextionary.creativesoftwarefdn.org/contextionary.json")

##
# Update the urls to the contextionary
##
for environment in data["environments"]:
    if environment["name"] == configToEdit:
        environment["contextionary"]["knn_file"] = "https://contextionary.creativesoftwarefdn.org/" + r.json()["latestVersion"] + "/en/contextionary.knn"
        environment["contextionary"]["idx_file"] = "https://contextionary.creativesoftwarefdn.org/" + r.json()["latestVersion"] + "/en/contextionary.idx"
    # append to newdata
    newData["environments"].append(environment)

##
# Write to file
##
with open(inputJson, 'w') as file:
    file.write(json.dumps(newData, indent=4))