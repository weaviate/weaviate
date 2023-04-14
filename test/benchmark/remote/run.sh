#!/bin/bash

PROJECT=semi-automated-benchmarking
ZONE=us-central1-a
INSTANCE=automated-loadtest
GOVERSION=https://go.dev/dl/go1.18.4.linux-amd64.tar.gz
FILE_PREFIX=${FILE_PREFIX:-""}

set -eou pipefail

# change to script directory
cd "${0%/*}" || exit

function main() {
  while [[ "$#" -gt 0 ]]; do
      case $1 in
          --all) run_all; exit 0 ;;
          --create_machine) create_machine; exit 0 ;;
          --clone_repository) clone_repository; exit 0;;
          --delete_machine) delete_machine; exit 0;;
          --install_dependencies) install_dependencies; exit 0;;
          --benchmark) benchmark; exit 0;;
          --prepare) prepare; exit 0;;
          --checkout) checkout "$2" ; exit 0;;
          --ssh) interactive_ssh; exit 0;;
          *) echo "Unknown parameter passed: $1"; exit 1 ;;
      esac
      shift
  done

  print_help
}

function print_help() {
  echo "Valid arguments include:"
  echo ""
  echo "  --all               Run everything, including machine creation & destruction"
  echo "  --prepare           Create Machine & run all the steps prior to benchmark execution"
  echo "  --create_machine    Only create machine"
  echo "  --delete_machine    Stop & Delete running machine"
  echo "  --clone_repository  Clone and checkout Weaviate repo at specified commit"
  echo "  --checkout          Checkout arbitrary branch or commit"
  echo "  --ssh               Interactive SSH session"
}

function run_all() {
  trap delete_machine EXIT

  prepare
  benchmark
}

function prepare() {
  check
  create_machine
  install_dependencies
  clone_repository
}

function check() {
  echo_green "Checking required dependencies"
  if ! command -v gcloud &> /dev/null
  then
      echo_red "Missing gcloud binary"
      return 1
  fi

  if ! command -v terraform &> /dev/null
  then
      echo_red "Missing terraform binary"
      return 1
  fi

  echo "Ready to go!"
}

function create_machine() {
  (cd terraform && terraform init)
  (cd terraform && terraform apply -auto-approve)
  echo "Sleeping for 10s, so first ssh doesn't fail. This should be improved through polling"
  sleep 10
}

function delete_machine() {
  (cd terraform && terraform destroy -auto-approve)
}

function install_dependencies() {
  ssh_command "sudo apt-get update && sudo apt-get install -y git git-lfs curl"
  ssh_command "ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts"
  install_go
  install_docker
}

function install_go {
  ssh_command "curl -Lo go.tar.gz $GOVERSION"
  ssh_command "sudo rm -rf /usr/local/go && sudo tar -C /usr/local -xzf go.tar.gz"
  ssh_command 'echo '"'"'PATH=$PATH:/usr/local/go/bin'"'"' >> ~/.profile'
  ssh_command "go version"
}

function install_docker() {
  ssh_command "if ! command -v docker &> /dev/null; then curl -fsSL https://get.docker.com -o get-docker.sh && sh ./get-docker.sh; fi"
  ssh_command "sudo groupadd docker || true"
  ssh_command "sudo usermod -aG docker $USER"
}

function clone_repository() {
  ref=$(git rev-parse --abbrev-ref HEAD)
  echo_green "Cloning weaviate repo to branch $ref"
  ssh_command "cd; [ ! -d weaviate ] && git clone --depth 1 --branch $ref https://github.com/weaviate/weaviate.git weaviate || true"
  ssh_command "cd weaviate; git-lfs install; git-lfs pull"
}

function checkout() {
  ref="$1"
  ssh_command "cd weaviate; git checkout $ref"
}

function benchmark() {
  echo_green "Run benchmarks on remote machine"
  ssh_command "echo "stop all running docker containers"; docker rm -f $(docker ps -q) || true"
  ssh_command "cd ~/weaviate; rm test/benchmark/benchmark_results.json || true"
  ssh_command "cd ~/weaviate; test/benchmark/run_performance_tracker.sh"
  echo_green "Copy results file to local machine"
  filename="${FILE_PREFIX}benchmark_results_$(date +%s).json"
  scp_command "$INSTANCE:~/weaviate/test/benchmark/benchmark_results.json" "$filename"
  echo "Results file succesfully copied to ${PWD}/$filename"
}


function echo_green() {
  green='\033[0;32m'
  nc='\033[0m' 
  echo -e "${green}${*}${nc}"
}

function echo_red() {
  red='\033[0;31m'
  nc='\033[0m' 
  echo -e "${red}${*}${nc}"
}

function ssh_command() {
  gcloud beta compute ssh --project=$PROJECT --zone=$ZONE  "$INSTANCE" --command="source ~/.profile; $1"
}

function scp_command() {
  gcloud beta compute scp --project=$PROJECT --zone=$ZONE  "$@"
}

function interactive_ssh() {
  gcloud beta compute ssh --project=$PROJECT --zone=$ZONE  "$INSTANCE"
}

main "$@"
