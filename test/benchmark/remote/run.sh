#!/usr/bin/env bash

set -eou pipefail

function main() {
  while [[ "$#" -gt 0 ]]; do
      case $1 in
          --all) run_all; exit 0 ;;
          *) echo "Unknown parameter passed: $1"; exit 1 ;;
      esac
      shift
  done

  print_help
}

function print_help() {
  echo "Valid arguments include:"
  echo ""
  echo "  --all      Run everything, including machine creation & destruction"
}

function run_all() {
  trap delete_machine EXIT

  check
  create_machine
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
}

function delete_machine() {
  (cd terraform && terraform destroy -auto-approve)
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

main "$@"
