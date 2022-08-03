This folder contains scripts to help execute the benchmark remotely on a GCP
VM.

## Requirements

- Bash
- gcloud (logged in and permissions to `semi-automated-benchmarking` project
- terraform

## Usage

From the root folder run

```
test/benchmark/remote/run.sh --all
```

To run the whole benchmark suite. The `--all` command will:

- Check that `gcloud` and `terraform` are installed locally
- Spin up a machine in GCP
- Install the dependencies on this machine
- Clone Weaviate
- Check out the same commit that is check out locally
- Run the benchmarks script
- Copy the results file to the local machine
- Destroy the machine

If any of the command fails the machine will be destroyed.

## Debugging

You can also run all commands individually, simply run the command without
parameters to list all possible options.

To do all steps (including spinning up a machine) that happen _before_ running
the benchmarks you can invoke it with the `--prepare` option. To get an
interactive ssh session into the machine, use `--ssh`. To destroy the machine
at an arbitrary point run `--delete_machine`.

## Run on arbitrary branch

*Note: Checking out a branch that does not yet have the scripts from this
folder, will fail. You need a branch created after this script was initially
built or has been rebased on top of it.*

```
test/benchmark/remote/run.sh --prepare
test/benchmark/remote/run.sh --checkout <name-of-your-branch-or-commit>
test/benchmark/remote/run.sh --benchmark
test/benchmark/remote/run.sh --delete_machine
```
