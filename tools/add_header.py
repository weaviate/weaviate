#!/usr/bin/env python3
import os
import glob
import re
import subprocess

ROOT_PATH = os.path.dirname(os.path.dirname(os.path.realpath(__file__)))

# These glob patterns relative to the ROOT_PATH will be searched for files
PATTERNS = [
    "{}/**/*.go"
]

FILES_TO_ADD_HEADERS_TO = []
for pattern in PATTERNS:
    FILES_TO_ADD_HEADERS_TO += glob.glob(pattern.format(ROOT_PATH), recursive=True)

with open("{}/tools/header.txt".format(ROOT_PATH), "r") as f:
    HEADER=f.read()

REGEX=re.compile(r"^/\*(.*\n \*)+/",re.M)
for file in FILES_TO_ADD_HEADERS_TO:
    with open(file, "r") as f:
        content = f.read()

    if REGEX.match(content):
        print("Updating header of", file)
        content = REGEX.sub(HEADER, content)
    else:
        print("Adding header to", file)
        content = HEADER + content.lstrip()

    with open(file, "w") as f:
        f.write(content)

subprocess.call(["gofmt", "-w", "."])
