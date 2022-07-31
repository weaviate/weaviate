#!/usr/bin/env bash

PROJECT=semi-automated-benchmarking
ZONE=us-central1-a
INSTANCE=automated-loadtest

set -eou pipefail

function main() {
  while [[ "$#" -gt 0 ]]; do
      case $1 in
          --all) run_all; exit 0 ;;
          --create_machine) create_machine; exit 0 ;;
          --clone_repository) clone_repository; exit 0;;
          --delete_machine) delete_machine; exit 0;;
          --install_dependencies) install_dependencies; exit 0;;
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
  echo "  --create_machine    Only create machine"
  echo "  --delete_machine    Stop & Delete running machine"
  echo "  --clone_repository  Clone and checkout Weaviate repo at specified commit"
}

function run_all() {
  trap delete_machine EXIT

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
  ssh_command "sudo apt-get update && sudo apt-get install -y git"
  ssh_command "ssh-keyscan -t rsa github.com >> ~/.ssh/known_hosts"
}

function clone_repository() {
  ssh_command "cd; [ ! -d weaviate ] && git clone https://github.com/semi-technologies/weaviate.git weaviate || true"
  ref=$(git rev-parse HEAD | head -c 7)
  echo_green "Checking out local commit/branch $ref"
  ssh_command "cd weaviate; git checkout $ref"

}


function echo_green() {
  green='\033[0;32m'
  nc='\033[0m' 
  echo -e "${green}${*}${nc}"
}

function echo_red() {
  green='\033[0;31m'
  nc='\033[0m' 
  echo -e "${green}${*}${nc}"
}

function ssh_command() {
  gcloud beta compute ssh --project=$PROJECT --zone=$ZONE  "$INSTANCE" --command="$1"
}

main "$@"
