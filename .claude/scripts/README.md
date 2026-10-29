# CI monitoring scripts

Run these as background tasks (`run_in_background=true` in the Bash tool) so the session is notified on
completion without blocking the conversation.

## Monitor PR checks

Polls all CI checks for a PR until they complete. Exits with code 1 if any checks fail:

```bash
PR=1234 .claude/scripts/monitor_pr.sh
```

## Monitor Docker image build

Use this when waiting for a PR's docker image to be produced. First get the run ID from the PR's docker
checks, then pass it to the script:

```bash
# Get the run ID
gh pr checks <PR> --repo weaviate/weaviate 2>&1 | grep -i "docker"
# Monitor the build
.claude/scripts/monitor_docker.sh <run_id>
```

The script also prints the docker image tags on success. Tags are fetched via `gh api` (works even if
the overall run is still in progress).

## Inspect failed checks / rerun

```bash
# Get job ID from: gh pr checks <PR> --repo weaviate/weaviate
gh api repos/weaviate/weaviate/actions/jobs/<job_id>/logs 2>&1 | grep "FAIL:" | head -20

# Re-run only failed jobs:
gh run rerun <run_id> --failed --repo weaviate/weaviate
```
