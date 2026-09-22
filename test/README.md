## Testing 


we use shell to run our (unit/ acceptance / integration) tests 

```shell
./test/run.sh ${command}
```
run that command with any of the following available commands, make sure you're in the project root folder.

### available commands 
 - `--unit-only` | `-u`
 - `--unit-and-integration-only` | `-ui`
 - `--integration-only` | `-i`
 - `--acceptance-only` | `-a`
 - `--acceptance-only-fast` | `-aof`
 - `--acceptance-only-graphql` | `-aog`
 - `--acceptance-only-replication` | `-aor`
 - `--acceptance-only-async-replication` | `-aoar`
 - `--acceptance-module-tests-only` | `--modules-only` | `-m`
 - `--acceptance-module-tests-only-backup` | `--modules-backup-only` | `-mob`
 - `--acceptance-module-tests-except-backup` | `--modules-except-backup` | `-meb`
 - `--only-module-{moduleName}`

### JUnit XML output

Set `JUNIT_DIR` to collect machine-readable test results (used by CI to report
results; a no-op when unset):

```shell
JUNIT_DIR=$PWD/junit ./test/run.sh --unit-only
```

Each `go test` invocation then runs through [gotestsum](https://github.com/gotestyourself/gotestsum)
(installed on demand, pinned in `test/tools/gotest_junit.sh`) and writes one
`go-<pid>-<seq>.xml` file into `JUNIT_DIR`; the pytest suite writes
`pytest-<mode>.xml`. Flags, exit codes, and console output are unchanged.
The directory's XML files are wiped at the start of every `run.sh` invocation,
so a CI retry reports only the final attempt.