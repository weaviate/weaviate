#!/bin/bash
# The pre-registered comparison of 64 MB against 128 MB (bench-report.md of
# feat/posting-edge-index, e91ea884e4): six processes, alternated, one binary.
# Second launch: the first binary ran no test (its file ended in _arm_test.go,
# a GOARCH=arm constraint), renamed to bench_pending_runs_test.go.
set -u
S=<scratch>
B=$S/budget-arms/runs.test
O=$S/budget-arms/r2
mkdir -p $O
cd $S/measure2/adapters/repos/db/vector/postingedge || exit 2
i=0
for budget in 372800 745600 372800 745600 372800 745600; do
  i=$((i+1))
  date "+== %H:%M:%S run $i budget $budget"
  PE_BUDGET=$budget PE_OUT=$O/run$i-$budget.json $B -test.run '^TestMeasurePendingBudgetArm$' -test.v -test.timeout 60m > $O/run$i-$budget.log 2>&1
  echo "run $i exit $?"; grep -E 'budget [0-9-]+:' $O/run$i-$budget.log | sed 's/^ *//'
done
date "+== %H:%M:%S done"
