import subprocess
import time

# This measures the start time of weaviate
#
# How to use:
#  - Build a container of the weaviate code that you want to benchmark, this script assumes that you use
#      ./tools/test/run_ci_server.sh
#  - Import objects
#  - Run this script, it will automatically stop/start the weaviate docker container multiple times and measure how long
#     it takes

runs = 10
container_name = "weaviate-weaviate-1"
total_time = 0

# stop any running weaviate containers
subprocess.run(["docker", "stop", container_name])

for _ in range(runs):
    start = time.time()
    subprocess.run(["docker", "start", container_name])

    # Loop until weaviate is responding
    while True:
        n = subprocess.run(["curl", "-s", "http://localhost:8080"])
        if n.returncode == 0:
            total_time += time.time()-start
            subprocess.run(["docker", "stop", container_name])
            break

print("Average time until weaviate responds: ", total_time/runs)
