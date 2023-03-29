
# GSI Technology's Gemini Fast Vector Search (FVS)

## Introduction

The Weaviate Gemini Plugin leverages GSI Technologies FVS Software Library.

This directory provides profiling, debugging, and benchmark utilities and scripts for FVS.

## Benchmarks

### Deep1B

![Subsets of Deep1B](results/gemini_fvs_deep1B.png)

Note: The following datasets are not shown yet:
* 100M of Deep1B
* 250M of Deep1B
* 500M of Deep1B
* All of Deep1B

## Index Build/Training Time

![Subsets of Deep1B](results/deep1B_master_train_time.png)

![Subsets of Deep1B - All Runs](results/Deep1B_subplots_train_time.png)

### Reproducing The Benchmarks

To reproduce the benchmarks shown above, do the following:
* login to a system with Gemini FVS installed
* create the data using the "make_datasets.py" file in this directory
* create or locate an FVS "allocation" and copy its id for use later
* configure the top of these scripts and run them:
  * [run_benchmarks_deep1M_q10.sh](run_benchmarks_deep1M_q10.sh)
  * [run_benchmarks_deep1M_q100.sh](run_benchmarks_deep1M_q100.sh)
  * [run_benchmarks_deep1M_q1000.sh](run_benchmarks_deep1M_q1000.sh)
  * [run_benchmarks_deep2M_q10.sh](run_benchmarks_deep2M_q10.sh)
  * [run_benchmarks_deep2M_q100.sh](run_benchmarks_deep2M_q100.sh)
  * [run_benchmarks_deep2M_q1000.sh](run_benchmarks_deep2M_q1000.sh)
  * [run_benchmarks_deep5M_q10.sh](run_benchmarks_deep5M_q10.sh)
  * [run_benchmarks_deep5M_q100.sh](run_benchmarks_deep5M_q100.sh)
  * [run_benchmarks_deep5M_q1000.sh](run_benchmarks_deep5M_q1000.sh)
  * [run_benchmarks_deep10M_q10.sh](run_benchmarks_deep10M_q10.sh)
  * [run_benchmarks_deep10M_q100.sh](run_benchmarks_deep10M_q100.sh)
  * [run_benchmarks_deep10M_q1000.sh](run_benchmarks_deep10M_q1000.sh)
  * [run_benchmarks_deep20M_q10.sh](run_benchmarks_deep20M_q10.sh)
  * [run_benchmarks_deep20M_q100.sh](run_benchmarks_deep20M_q100.sh)
  * [run_benchmarks_deep20M_q1000.sh](run_benchmarks_deep20M_q1000.sh)
  * [run_benchmarks_deep50M_q10.sh](run_benchmarks_deep50M_q10.sh)
  * [run_benchmarks_deep50M_q100.sh](run_benchmarks_deep50M_q100.sh)
* to plot your data, use the following notebooks:
  * [recall vs throughput notebook](train_time_analysis.ipynb)
  * [training time notebook](benchmarks_analysis.ipynb)
* we tested with python 3.8 so recommend you use that version



