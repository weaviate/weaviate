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