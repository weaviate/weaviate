import argparse
import logging
import time
import resource
import pdb

import numpy as np

import faiss

from faiss.contrib.exhaustive_search import range_search_gpu

##import benchmark.datasets
#from benchmark.datasets import DATASETS
import datasets
datasets.BASEDIR = "/mnt/nas1/fvs_benchmark_datasets/bigann_competition_data"

"""
for dataset in deep-1B bigann-1B ssnpp-1B text2image-1B msturing-1B msspacev-1B ; do
    sbatch   --gres=gpu:4 --ntasks=1 --time=30:00:00 --cpus-per-task=40        \
           --partition=learnlab --mem=250g --nodes=1  \
           -J GT.100M.$dataset.d -o logs/GT.100M.$dataset.d.log \
           --wrap "PYTHONPATH=. python dataset_preparation/make_groundtruth.py \
            --dataset $dataset --split 10 0 --prepare \
            --o /checkpoint/matthijs/billion-scale-ann-benchmarks/GT_100M/${dataset/1B/100M}
        "
done

"""


class ResultHeap:
    """Accumulate query results from a sliced dataset. The final result will
    be in self.D, self.I."""

    def __init__(self, nq, k, keep_max=False):
        " nq: number of query vectors, k: number of results per query "
        self.I = np.zeros((nq, k), dtype='int64')
        self.D = np.zeros((nq, k), dtype='float32')
        self.nq, self.k = nq, k
        if keep_max:
            heaps = faiss.float_minheap_array_t()
        else:
            heaps = faiss.float_maxheap_array_t()
        heaps.k = k
        heaps.nh = nq
        heaps.val = faiss.swig_ptr(self.D)
        heaps.ids = faiss.swig_ptr(self.I)
        heaps.heapify()
        self.heaps = heaps

    def add_result(self, D, I):
        """D, I do not need to be in a particular order (heap or sorted)"""
        assert D.shape == (self.nq, self.k)
        assert I.shape == (self.nq, self.k)
        self.heaps.addn_with_ids(
            self.k, faiss.swig_ptr(D),
            faiss.swig_ptr(I), self.k)

    def finalize(self):
        self.heaps.reorder()


def knn_ground_truth(ds, k, bs, split):
    """Computes the exact KNN search results for a dataset that possibly
    does not fit in RAM but for which we have an iterator that
    returns it block by block.
    """
    print("loading queries")
    xq = ds.get_queries()
    print("knn_ground_truth: queries size", xq.shape)

    #GW
    #print("HACK truncating to 1000")
    #xq = xq[0:1000,:]
    #GW

    if ds.distance() == "angular":
        faiss.normalize_L2(xq)

    print("knn_ground_truth queries size %s k=%d" % (xq.shape, k))

    t0 = time.time()
    nq, d = xq.shape

    print("DF distance", ds.distance())

    metric_type = (
        faiss.METRIC_L2 if ds.distance() == "euclidean" else
        faiss.METRIC_INNER_PRODUCT if ds.distance() in ("ip", "angular") else
        1/0
    )
    rh = ResultHeap(nq, k, keep_max=metric_type == faiss.METRIC_INNER_PRODUCT)

    index = faiss.IndexFlat(d, metric_type)

    if faiss.get_num_gpus():
        print('running on %d GPUs' % faiss.get_num_gpus())
        index = faiss.index_cpu_to_all_gpus(index)

    # compute ground-truth by blocks, and add to heaps
    i0 = 0
    for xbi in ds.get_dataset_iterator(bs=bs, split=split):
        ni = xbi.shape[0]
        if ds.distance() == "angular":
            faiss.normalize_L2(xbi)

        index.add(xbi)
        D, I = index.search(xq, k)
        I += i0
        rh.add_result(D, I)
        index.reset()
        i0 += ni
        print(f"[{time.time() - t0:.2f} s] {i0} / {ds.nb} vectors", end="\r", flush=True)

    rh.finalize()
    print()
    print("GT time: %.3f s (%d vectors)" % (time.time() - t0, i0))

    return rh.D, rh.I


def range_ground_truth(ds, radius, bs, split):
    """Computes the exact range search results for a dataset that possibly
    does not fit in RAM but for which we have an iterator that
    returns it block by block.
    """
    print("loading queries")
    xq = ds.get_queries()

    if ds.distance() == "angular":
        faiss.normalize_L2(xq)

    print("range_ground_truth queries size %s radius=%g" % (xq.shape, radius))

    t0 = time.time()
    nq, d = xq.shape

    metric_type = (
        faiss.METRIC_L2 if ds.distance() == "euclidean" else
        faiss.METRIC_INNER_PRODUCT if ds.distance() in ("ip", "angular") else
        1/0
    )

    index = faiss.IndexFlat(d, metric_type)

    if faiss.get_num_gpus():
        print('running on %d GPUs' % faiss.get_num_gpus())
        index_gpu = faiss.index_cpu_to_all_gpus(index)
    else:
        index_gpu = None

    results = []

    # compute ground-truth by blocks, and add to heaps
    i0 = 0
    tot_res = 0
    for xbi in ds.get_dataset_iterator(bs=bs, split=split):
        ni = xbi.shape[0]
        if ds.distance() == "angular":
            faiss.normalize_L2(xbi)

        index.add(xbi)
        if index_gpu is None:
            lims, D, I = index.range_search(xq, radius)
        else:
            index_gpu.add(xbi)
            lims, D, I = range_search_gpu(xq, radius, index_gpu, index)
            index_gpu.reset()
        index.reset()
        I = I.astype("int32")
        I += i0
        results.append((lims, D, I))
        i0 += ni
        tot_res += len(D)
        print(f"[{time.time() - t0:.2f} s] {i0} / {ds.nb} vectors, {tot_res} matches",
            end="\r", flush=True)
    print()
    print("merge into single table")
    # merge all results in a single table
    nres = np.zeros(nq, dtype="int32")
    D = []
    I = []
    for q in range(nq):
        nres_q = 0
        for lims_i, Di, Ii in results:
            l0, l1 = lims_i[q], lims_i[q + 1]
            if l1 > l0:
                nres_q += l1 - l0
                D.append(Di[l0:l1])
                I.append(Ii[l0:l1])
        nres[q] = nres_q

    D = np.hstack(D)
    I = np.hstack(I)
    assert len(D) == nres.sum() == len(I)
    print("GT time: %.3f s (%d vectors)" % (time.time() - t0, i0))
    return nres, D, I

def usbin_write(ids, dist, fname):
    ids = np.ascontiguousarray(ids, dtype="int32")
    dist = np.ascontiguousarray(dist, dtype="float32")
    assert ids.shape == dist.shape
    f = open(fname, "wb")
    n, d = dist.shape
    np.array([n, d], dtype='uint32').tofile(f)
    ids.tofile(f)
    dist.tofile(f)


def range_result_write(nres, I, D, fname):
    """ write the range search file format:
    int32 n_queries
    int32 total_res
    int32[n_queries] nb_results_per_query
    int32[total_res] database_ids
    float32[total_res] distances
    """
    nres = np.ascontiguousarray(nres, dtype="int32")
    I = np.ascontiguousarray(I, dtype="int32")
    D = np.ascontiguousarray(D, dtype="float32")
    assert I.shape == D.shape
    total_res = nres.sum()
    nq = len(nres)
    assert I.shape == (total_res, )
    f = open(fname, "wb")
    np.array([nq, total_res], dtype='uint32').tofile(f)
    nres.tofile(f)
    I.tofile(f)
    D.tofile(f)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()

    def aa(*args, **kwargs):
        group.add_argument(*args, **kwargs)

    group = parser.add_argument_group('dataset options')
    aa('--dataset', choices=datasets.DATASETS.keys(), required=True)
    aa('--prepare', default=False, action="store_true",
        help="call prepare() to download the dataset before computing")
    aa('--basedir', help="override basedir for dataset")
    aa('--split', type=int, nargs=2, default=[1, 0],
        help="split that must be handled")

    group = parser.add_argument_group('computation options')
    # determined from ds
    # aa('--range_search', action="store_true", help="do range search instead of kNN search")
    aa('--k', default=100, type=int, help="number of nearest kNN neighbors to search")
    aa('--radius', default=96237, type=float, help="range search radius")
    aa('--bs', default=100_000, type=int, help="batch size for database iterator")
    aa("--maxRAM", default=100, type=int, help="set max RSS in GB (avoid OOM crash)")

    group = parser.add_argument_group('output options')
    aa('--o', default="", help="output file name")
    #GW
    aa('--numpy',default=False,help="use numpy format")
    #GW
    args = parser.parse_args()

    print("args:", args)
    print("numpy", args.numpy)

    if args.basedir:
        print("setting datasets basedir to", args.basedir)
        benchmark.datasets.BASEDIR
        benchmark.datasets.BASEDIR = args.basedir

    if args.maxRAM > 0:
        print("setting max RSS to", args.maxRAM, "GiB")
        resource.setrlimit(
            resource.RLIMIT_DATA, (args.maxRAM * 1024 ** 3, resource.RLIM_INFINITY)
        )

    ds = datasets.DATASETS[args.dataset]()

    print(ds)

    if args.prepare:
        print("downloading dataset...")
        ds.prepare()
        print("dataset ready")

    if False: # args.crop_nb != -1:
        print("cropping dataset to", args.crop_nb)
        ds.nb = args.crop_nb
        print("new ds:", ds)


    if ds.search_type() == "knn":
        D, I = knn_ground_truth(ds, k=args.k, bs=args.bs, split=args.split)
        print(f"writing index matrix of size {I.shape} to {args.o}")
        # write in the usbin format
        if args.numpy:
            print("Using numpy format for", I.shape)
            np.save(args.o,I)
        else:
            usbin_write(I, D, args.o)
    elif ds.search_type() == "range":
        nres, D, I = range_ground_truth(ds, radius=args.radius, bs=args.bs, split=args.split)
        print(f"writing results {I.shape} to {args.o}")
        range_result_write(nres, I, D, args.o)


