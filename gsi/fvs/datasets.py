import math
import numpy
import os
import random
import sys
import struct
import time
import traceback

import numpy as np

from urllib.request import urlopen
from urllib.request import urlretrieve

BASEDIR = "data/"

def download(src, dst=None, max_size=None):
    """ download an URL, possibly cropped """
    print("main download", dst, max_size)
    if os.path.exists(dst):
        return
    print('downloading %s -> %s...' % (src, dst))
    if max_size is not None:
        print("   stopping at %d bytes" % max_size)
    t0 = time.time()
    outf = open(dst, "wb")
    inf = urlopen(src)
    info = dict(inf.info())
    content_size = int(info['Content-Length'])
    bs = 1 << 20
    totsz = 0
    while True:
        block = inf.read(bs)
        elapsed = time.time() - t0
        print(
            "  [%.2f s] downloaded %.2f MiB / %.2f MiB at %.2f MiB/s   " % (
                elapsed,
                totsz / 2**20, content_size / 2**20,
                totsz / 2**20 / elapsed),
            flush=True, end="\r"
        )
        if not block:
            break
        print("download blocks", max_size, totsz, len(block))
        if max_size is not None and totsz + len(block) >= max_size:
            block = block[:max_size - totsz]
            outf.write(block)
            totsz += len(block)
            break
        outf.write(block)
        totsz += len(block)
    print()
    print("download finished in %.2f s, total size %d bytes" % (
        time.time() - t0, totsz
    ))


def download_accelerated(src, dst, quiet=False, sas_string=""):
    """ dowload using an accelerator. Make sure the executable is in the path """
    print('downloading %s -> %s...' % (src, dst))
    if "windows.net" in src:
        if sas_string == "":
            cmd = f"azcopy copy {src} {dst}"
        else:
            cmd = f"azcopy copy '{src}?{sas_string}' '{dst}'"
    else:
        cmd = f"axel --alternate -n 10 {src} -o {dst}"
        if quiet:
            cmd += " -q"

    print("running", cmd)
    ret = os.system(cmd)
    assert ret == 0

def upload_accelerated(local_dir, blob_prefix, component, sas_string, quiet=False):
    """ Upload index component to Azure blob using SAS string"""
    src = os.path.join(local_dir, component)
    dst = blob_prefix + '/' + component + '?' + sas_string
    print('Uploading %s -> %s...' % (src, dst))
    
    cmd = f"azcopy copy '{src}' '{dst}'"
    print("running", cmd)
    ret = os.system(cmd)
    assert ret == 0


def bvecs_mmap(fname):
    x = numpy.memmap(fname, dtype='uint8', mode='r')
    d = x[:4].view('int32')[0]
    return x.reshape(-1, d + 4)[:, 4:]

def ivecs_read(fname):
    a = numpy.fromfile(fname, dtype='int32')
    d = a[0]
    return a.reshape(-1, d + 1)[:, 1:].copy()

def xbin_mmap(fname, dtype, maxn=-1):
    """ mmap the competition file format for a given type of items """
    n, d = map(int, np.fromfile(fname, dtype="uint32", count=2))

    # HACK - to handle improper header in file for private deep-1B
    # if override_d and override_d != d:
    #    print("Warning: xbin_mmap map returned d=%s, but overridig with %d" % (d, override_d))
    #    d = override_d
    # HACK

    assert os.stat(fname).st_size == 8 + n * d * np.dtype(dtype).itemsize
    if maxn > 0:
        n = min(n, maxn)
    return np.memmap(fname, dtype=dtype, mode="r", offset=8, shape=(n, d))

def range_result_read(fname):
    """ read the range search result file format """
    f = open(fname, "rb")
    nq, total_res = np.fromfile(f, count=2, dtype="int32")
    nres = np.fromfile(f, count=nq, dtype="int32")
    assert nres.sum() == total_res
    I = np.fromfile(f, count=total_res, dtype="int32")
    D = np.fromfile(f, count=total_res, dtype="float32")
    return nres, I, D

def knn_result_read(fname):
    print("Trying to open knn_result file at", fname)
    n, d = map(int, np.fromfile(fname, dtype="uint32", count=2))
    assert os.stat(fname).st_size == 8 + n * d * (4 + 4)
    f = open(fname, "rb")
    f.seek(4+4)
    I = np.fromfile(f, dtype="int32", count=n * d).reshape(n, d)
    D = np.fromfile(f, dtype="float32", count=n * d).reshape(n, d)
    return I, D

def read_fbin(filename, start_idx=0, chunk_size=None):
    """ Read *.fbin file that contains float32 vectors
    Args:
        :param filename (str): path to *.fbin file
        :param start_idx (int): start reading vectors from this index
        :param chunk_size (int): number of vectors to read.
                                 If None, read all vectors
    Returns:
        Array of float32 vectors (numpy.ndarray)
    """
    with open(filename, "rb") as f:
        nvecs, dim = np.fromfile(f, count=2, dtype=np.int32)
        nvecs = (nvecs - start_idx) if chunk_size is None else chunk_size
        arr = np.fromfile(f, count=nvecs * dim, dtype=np.float32,
                          offset=start_idx * 4 * dim)
    return arr.reshape(nvecs, dim)


def read_ibin(filename, start_idx=0, chunk_size=None):
    """ Read *.ibin file that contains int32 vectors
    Args:
        :param filename (str): path to *.ibin file
        :param start_idx (int): start reading vectors from this index
        :param chunk_size (int): number of vectors to read.
                                 If None, read all vectors
    Returns:
        Array of int32 vectors (numpy.ndarray)
    """
    with open(filename, "rb") as f:
        nvecs, dim = np.fromfile(f, count=2, dtype=np.int32)
        nvecs = (nvecs - start_idx) if chunk_size is None else chunk_size
        arr = np.fromfile(f, count=nvecs * dim, dtype=np.int32,
                          offset=start_idx * 4 * dim)
    return arr.reshape(nvecs, dim)


def sanitize(x):
    return numpy.ascontiguousarray(x, dtype='float32')


class Dataset():
    def prepare(self):
        """
        Download and prepare dataset, queries, groundtruth.
        """
        pass
    def get_dataset_fn(self):
        """
        Return filename of dataset file.
        """
        pass
    def get_dataset(self):
        """
        Return memmapped version of the dataset.
        """
        pass
    def get_dataset_iterator(self, bs=512, split=(1, 0)):
        """
        Return iterator over blocks of dataset of size at most 512.
        The split argument takes a pair of integers (n, p) where p = 0..n-1
        The dataset is split in n shards, and the iterator returns only shard #p
        This makes it possible to process the dataset independently from several
        processes / threads.
        """
        pass
    def get_queries(self):
        """
        Return (nq, d) array containing the nq queries.
        """
        pass
    def get_private_queries(self):
        """
        Return (private_nq, d) array containing the private_nq private queries.
        """
        pass
    def get_groundtruth(self, k=None):
        """
        Return (nq, k) array containing groundtruth indices
        for each query."""
        pass

    def search_type(self):
        """
        "knn" or "range"
        """
        pass

    def distance(self):
        """
        "euclidean" or "ip" or "angular"
        """
        pass

    def default_count(self):
        return 10

    def short_name(self):
        return f"{self.__class__.__name__}-{self.nb}"
    
    def __str__(self):
        return (
            f"Dataset {self.__class__.__name__} in dimension {self.d}, with distance {self.distance()}, "
            f"search_type {self.search_type()}, size: Q {self.nq} B {self.nb}")


#############################################################################
# Datasets for the competition
##############################################################################



class DatasetCompetitionFormat(Dataset):
    """
    Dataset in the native competition format, that is able to read the
    files in the https://big-ann-benchmarks.com/ page.
    The constructor should set all fields. The functions below are generic.

    For the 10M versions of the dataset, the database files are downloaded in
    part and stored with a specific suffix. This is to avoid having to maintain
    two versions of the file.
    """

    def prepare(self, skip_data=False):
        if not os.path.exists(self.basedir):
            os.makedirs(self.basedir)

        if True:
            #print("Your problem could be here...")
            # start with the small ones...
            #print("public", self.qs_fn, self.gt_fn)
            for fn in [self.qs_fn, self.gt_fn]:
                if fn is None:
                    continue
                if fn.startswith("https://"):
                    sourceurl = fn
                    outfile = os.path.join(self.basedir, fn.split("/")[-1])
                else:
                    sourceurl = os.path.join(self.base_url, fn)
                    outfile = os.path.join(self.basedir, fn)
                if os.path.exists(outfile):
                    print("file %s already exists" % outfile)
                    continue
                print("download", sourceurl)
                try:
                    download(sourceurl, outfile)
                except:
                    print("Warning: Problem downloading->%s to %s" % (sourceurl, outfile) )
                    traceback.print_exc()
        else:
                print("Warning: Did NOT download public query at gt datasets.")

        # private qs url
        if self.private_qs_url:
            outfile = os.path.join(self.basedir, self.private_qs_url.split("/")[-1])
            if os.path.exists(outfile):
                print("private qs file %s already exists" % outfile)
            else:
                download(self.private_qs_url, outfile)
        
        # private gt url
        if self.private_gt_url:
            outfile = os.path.join(self.basedir, self.private_gt_url.split("/")[-1])
            if os.path.exists(outfile):
                print("private gt file %s already exists" % outfile)
            else:
                download(self.private_gt_url, outfile)

        if skip_data:
            return

        fn = self.ds_fn
        sourceurl = os.path.join(self.base_url, fn)
        outfile = os.path.join(self.basedir, fn)
        if os.path.exists(outfile):
            print("file %s already exists" % outfile)
            return
        if self.nb == 10**9:
            download_accelerated(sourceurl, outfile)
        else:
            # download cropped version of file
            file_size = 8 + self.d * self.nb * np.dtype(self.dtype).itemsize
            print("Download cropped ->", file_size)
            outfile = outfile + '.crop_nb_%d' % self.nb
            if os.path.exists(outfile):
                print("file %s already exists" % outfile)
                return
            download(sourceurl, outfile, max_size=file_size)
            # then overwrite the header...
            header = np.memmap(outfile, shape=2, dtype='uint32', mode="r+")
            assert header[0] == 10**9
            assert header[1] == self.d
            header[0] = self.nb

    def get_dataset_fn(self):
        fn = os.path.join(self.basedir, self.ds_fn)
        if os.path.exists(fn):
            return fn
        if self.nb != 10**9:
            fn += '.crop_nb_%d' % self.nb
            return fn
        else:
            raise RuntimeError("file not found")

    def get_dataset_iterator(self, bs=512, split=(1,0)):
        nsplit, rank = split
        i0, i1 = self.nb * rank // nsplit, self.nb * (rank + 1) // nsplit
        filename = self.get_dataset_fn()
        print("Get dataset iterator for", filename)
        x = xbin_mmap(filename, dtype=self.dtype, maxn=self.nb)
        assert x.shape == (self.nb, self.d)
        for j0 in range(i0, i1, bs):
            j1 = min(j0 + bs, i1)
            yield sanitize(x[j0:j1])

    def search_type(self):
        return "knn"

    def get_groundtruth(self, k=None):
        #print("get groundtruth", self.gt_fn)
        assert self.gt_fn is not None
        fn = self.gt_fn.split("/")[-1]   # in case it's a URL
        #print("FN",fn)
        assert self.search_type() == "knn"

        #print("local file", os.path.join(self.basedir, fn))

        I, D = knn_result_read(os.path.join(self.basedir, fn))
        #print("knn result read result", type(I), type(D), I.shape, D.shape)
        assert I.shape[0] == self.nq
        if k is not None:
            assert k <= 100
            I = I[:, :k]
            D = D[:, :k]
        return I, D

    def get_dataset(self):
        assert self.nb <= 10**7, "dataset too large, use iterator"
        return sanitize(next(self.get_dataset_iterator(bs=self.nb)))

    def get_queries(self):
        #print("QSFN", self.qs_fn)
        filename = os.path.join(self.basedir, self.qs_fn)
        #print("FNAME", filename)
        x = xbin_mmap(filename, dtype=self.dtype)
        assert x.shape == (self.nq, self.d)
        return sanitize(x)

    def get_private_queries(self):
        ##print("gpq", self.private_qs_url)
        assert self.private_qs_url is not None
        fn = self.private_qs_url.split("/")[-1]   # in case it's a URL
        filename = os.path.join(self.basedir, fn)
        x = xbin_mmap(filename, dtype=self.dtype)
        assert x.shape == (self.private_nq, self.d)
        return sanitize(x)
    
    def get_private_groundtruth(self, k=None):
        #print("gpg", self.private_gt_url)
        assert self.private_gt_url is not None
        fn = self.private_gt_url.split("/")[-1]   # in case it's a URL
        assert self.search_type() == "knn"

        I, D = knn_result_read(os.path.join(self.basedir, fn))
        assert I.shape[0] == self.private_nq
        if k is not None:
            assert k <= 100
            I = I[:, :k]
            D = D[:, :k]
        return I, D

subset_url = "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/"

class SSNPPDataset(DatasetCompetitionFormat):
    def __init__(self, nb_M=1000):
        # assert nb_M in (10, 1000)
        self.nb_M = nb_M
        self.nb = 10**6 * nb_M
        self.d = 256
        self.nq = 100000
        self.dtype = "uint8"
        self.ds_fn = "FB_ssnpp_database.u8bin"
        self.qs_fn = "FB_ssnpp_public_queries.u8bin"
        self.gt_fn = (
            "FB_ssnpp_public_queries_1B_GT.rangeres" if self.nb_M == 1000 else
            subset_url + "GT_100M/ssnpp-100M" if self.nb_M == 100 else
            subset_url + "GT_10M/ssnpp-10M" if self.nb_M == 10 else
            None
        )

        self.base_url = "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/"
        self.basedir = os.path.join(BASEDIR, "FB_ssnpp")

        self.private_nq = 100000   
        self.private_qs_url = "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/FB_ssnpp_heldout_queries_3307fba121460a56.u8bin"
        self.private_gt_url = "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/GT_1B_final_2bf4748c7817/FB_ssnpp.bin"

    def search_type(self):
        return "range"

    def default_count(self):
        return 96237

    def distance(self):
        return "euclidean"

    def get_groundtruth(self, k=None):
        """ override the ground-truth function as this is the only range search dataset """
        assert self.gt_fn is not None
        fn = self.gt_fn.split("/")[-1]   # in case it's a URL
        return range_result_read(os.path.join(self.basedir, fn))
    
    def get_private_groundtruth(self, k=None):
        """ override the ground-truth function as this is the only range search dataset """
        assert self.private_gt_url is not None
        fn = self.private_gt_url.split("/")[-1]   # in case it's a URL
        return range_result_read(os.path.join(self.basedir, fn))

class BigANNDataset(DatasetCompetitionFormat):
    def __init__(self, nb_M=1000):
        self.nb_M = nb_M
        self.nb = 10**6 * nb_M
        self.d = 128
        self.nq = 10000
        self.dtype = "uint8"
        self.ds_fn = "base.1B.u8bin"
        self.qs_fn = "query.public.10K.u8bin"
        self.gt_fn = (
            "GT.public.1B.ibin" if self.nb_M == 1000 else
            subset_url + "GT_100M/bigann-100M" if self.nb_M == 100 else
            subset_url + "GT_10M/bigann-10M" if self.nb_M == 10 else
            None
        )
        # self.gt_fn = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/bigann/public_query_gt100.bin" if self.nb == 10**9 else None
        self.base_url = "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/"
        self.basedir = os.path.join(BASEDIR, "bigann")
        
        self.private_nq = 10000   
        self.private_qs_url = "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/bigann/query.private.799253207.10K.u8bin"
        self.private_gt_url = "https://dl.fbaipublicfiles.com/billion-scale-ann-benchmarks/GT_1B_final_2bf4748c7817/bigann-1B.bin"


    def distance(self):
        return "euclidean"

class Deep1BDataset(DatasetCompetitionFormat):
    def __init__(self, nb_M=1000):
        self.nb_M = nb_M
        if (self.nb_M):
            print("Warning: Requested size is not a multiple of 1M")
        self.nb = int( 10**6 * nb_M )
        self.d = 96
        self.nq = 10000
        self.dtype = "float32"
        self.ds_fn = "base.1B.fbin"
        self.qs_fn = "query.public.10K.fbin"
        #print("D!B", self.qs_fn)
        self.gt_fn = (
            "https://storage.yandexcloud.net/yandex-research/ann-datasets/deep_new_groundtruth.public.10K.bin" if self.nb_M == 1000 else
            subset_url + "GT_100M/deep-100M" if self.nb_M == 100 else
            subset_url + "GT_50M/deep-50M" if self.nb_M == 50 else
            subset_url + "GT_20M/deep-20M" if self.nb_M == 20 else
            subset_url + "GT_10M/deep-10M" if self.nb_M == 10 else
            subset_url + "GT_10M/deep-5M" if self.nb_M == 5 else
            subset_url + "GT_10M/deep-2M" if self.nb_M == 2 else
            subset_url + "GT_1M/deep-1M" if self.nb_M == 1 else
            subset_url + "GT_1M/deep-10K" if self.nb_M == 0.01 else
            None
        )
        self.base_url = "https://storage.yandexcloud.net/yandex-research/ann-datasets/DEEP/"
        self.basedir = os.path.join(BASEDIR, "deep1b")

        self.private_nq = 30000   
        self.private_qs_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/deep1b/query.heldout.30K.fbin"
        self.private_gt_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/deep1b/gt100-heldout.30K.fbin"

        self.private_nq_large = 1000000
        self.private_qs_large_url = "https://storage.yandexcloud.net/yr-secret-share/ann-datasets-5ac0659e27/DEEP/query.private.1M.fbin"

    def distance(self):
        return "euclidean"



class Text2Image1B(DatasetCompetitionFormat):
    def __init__(self, nb_M=1000):
        self.nb_M = nb_M
        self.nb = 10**6 * nb_M
        self.d = 200
        self.nq = 100000
        self.dtype = "float32"
        self.ds_fn = "base.1B.fbin"
        self.qs_fn = "query.public.100K.fbin"
        self.gt_fn = (
            "https://storage.yandexcloud.net/yandex-research/ann-datasets/t2i_new_groundtruth.public.100K.bin" if self.nb_M == 1000 else
            subset_url + "GT_100M/text2image-100M" if self.nb_M == 100 else
            subset_url + "GT_10M/text2image-10M" if self.nb_M == 10 else
            None
        )
        self.base_url = "https://storage.yandexcloud.net/yandex-research/ann-datasets/T2I/"
        self.basedir = os.path.join(BASEDIR, "text2image1B")

        self.private_nq = 30000   
        self.private_qs_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/text2image1b/query.heldout.30K.fbin"
        self.private_gt_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/text2image1b/gt100-heldout.30K.fbin"

        self.private_nq_large = 1000000
        self.private_qs_large_url = "https://storage.yandexcloud.net/yr-secret-share/ann-datasets-5ac0659e27/T2I/query.private.1M.fbin"

    def distance(self):
        return "ip"

    def get_query_train(self, maxn=10**6):
        xq_train = np.memmap(
            BASEDIR + "/text2image1B/query.learn.50M.fbin", offset=8,
            dtype='float32', shape=(maxn, 200), mode='r')
        return np.array(xq_train)

class MSTuringANNS(DatasetCompetitionFormat):
    def __init__(self, nb_M=1000):
        self.nb_M = nb_M
        self.nb = 10**6 * nb_M
        self.d = 100
        self.nq = 100000
        self.dtype = "float32"
        self.ds_fn = "base1b.fbin"
        self.qs_fn = "query100K.fbin"
        self.gt_fn = (
            "query_gt100.bin" if self.nb_M == 1000 else
            subset_url + "GT_100M/msturing-100M" if self.nb_M == 100 else
            subset_url + "GT_10M/msturing-10M" if self.nb_M == 10 else
            None
        )
        self.base_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/MSFT-TURING-ANNS/"
        self.basedir = os.path.join(BASEDIR, "MSTuringANNS")

        self.private_nq = 10000
        self.private_qs_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/MSFT-TURING-ANNS/testQuery10K.fbin"
        self.private_gt_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/MSFT-TURING-ANNS/gt100-private10K-queries.bin"

        self.private_nq_large = 99605
        self.private_qs_large_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/MSFT-TURING-ANNS/testQuery99605.fbin"
        self.private_gt_large_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/MSFT-TURING-ANNS/gt100-private99605-queries.bin"

    def distance(self):
        return "euclidean"


class MSSPACEV1B(DatasetCompetitionFormat):
    def __init__(self, nb_M=1000):
        self.nb_M = nb_M
        self.nb = 10**6 * nb_M
        self.d = 100
        self.nq = 29316
        self.dtype = "int8"
        self.ds_fn = "spacev1b_base.i8bin"
        self.qs_fn = "query.i8bin"
        self.gt_fn = (
            "public_query_gt100.bin" if self.nb_M == 1000 else
            subset_url + "GT_100M/msspacev-100M" if self.nb_M == 100 else
            subset_url + "GT_10M/msspacev-10M" if self.nb_M == 10 else
            None
        )
        self.base_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/spacev1b/"
        self.basedir = os.path.join(BASEDIR, "MSSPACEV1B")

        self.private_nq = 30000
        self.private_qs_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/spacev1b/private_query_30k.bin"
        self.private_gt_url = "https://comp21storage.blob.core.windows.net/publiccontainer/comp21/spacev1b/gt100_private_query_30k.bin"

    def distance(self):
        return "euclidean"

class RandomRangeDS(DatasetCompetitionFormat):
    def __init__(self, nb, nq, d):
        self.nb = nb
        self.nq = nq
        self.d = d
        self.dtype = 'float32'
        self.ds_fn = f"data_{self.nb}_{self.d}"
        self.qs_fn = f"queries_{self.nq}_{self.d}"
        self.gt_fn = f"gt_{self.nb}_{self.nq}_{self.d}"
        self.basedir = os.path.join(BASEDIR, f"random{self.nb}")
        if not os.path.exists(self.basedir):
            os.makedirs(self.basedir)

    def prepare(self, skip_data=False):
        import sklearn.datasets
        import sklearn.model_selection
        from sklearn.neighbors import NearestNeighbors

        print(f"Preparing datasets with {self.nb} random points and {self.nq} queries.")


        X, _ = sklearn.datasets.make_blobs(
            n_samples=self.nb + self.nq, n_features=self.d,
            centers=self.nq, random_state=1)

        data, queries = sklearn.model_selection.train_test_split(
            X, test_size=self.nq, random_state=1)


        with open(os.path.join(self.basedir, self.ds_fn), "wb") as f:
            np.array([self.nb, self.d], dtype='uint32').tofile(f)
            data.astype('float32').tofile(f)
        with open(os.path.join(self.basedir, self.qs_fn), "wb") as f:
            np.array([self.nq, self.d], dtype='uint32').tofile(f)
            queries.astype('float32').tofile(f)

        print("Computing groundtruth")

        nbrs = NearestNeighbors(n_neighbors=100, metric="euclidean", algorithm='brute').fit(data)
        D, I = nbrs.kneighbors(queries)

        nres = np.count_nonzero((D < math.sqrt(self.default_count())) == True, axis=1)
        DD = np.zeros(nres.sum())
        II = np.zeros(nres.sum(), dtype='int32')

        s = 0
        for i, l in enumerate(nres):
            DD[s : s + l] = D[i, 0 : l]
            II[s : s + l] = I[i, 0 : l]
            s += l

        with open(os.path.join(self.basedir, self.gt_fn), "wb") as f:
            np.array([self.nq, nres.sum()], dtype='uint32').tofile(f)
            nres.astype('int32').tofile(f)
            II.astype('int32').tofile(f)
            DD.astype('float32').tofile(f)

    def get_groundtruth(self, k=None):
        """ override the ground-truth function as this is the only range search dataset """
        assert self.gt_fn is not None
        fn = self.gt_fn.split("/")[-1]   # in case it's a URL
        return range_result_read(os.path.join(self.basedir, fn))

    def search_type(self):
        return "range"

    def default_count(self):
        return 49

    def distance(self):
        return "euclidean"

    def __str__(self):
        return f"RandomRange({self.nb})"

class RandomDS(DatasetCompetitionFormat):
    def __init__(self, nb, nq, d):
        self.nb = nb
        self.nq = nq
        self.d = d
        self.dtype = 'float32'
        self.ds_fn = f"data_{self.nb}_{self.d}"
        self.qs_fn = f"queries_{self.nq}_{self.d}"
        self.gt_fn = f"gt_{self.nb}_{self.nq}_{self.d}"
        self.basedir = os.path.join(BASEDIR, f"random{self.nb}")
        if not os.path.exists(self.basedir):
            os.makedirs(self.basedir)

    def prepare(self, skip_data=False):
        import sklearn.datasets
        import sklearn.model_selection
        from sklearn.neighbors import NearestNeighbors

        print(f"Preparing datasets with {self.nb} random points and {self.nq} queries.")


        X, _ = sklearn.datasets.make_blobs(
            n_samples=self.nb + self.nq, n_features=self.d,
            centers=self.nq, random_state=1)

        data, queries = sklearn.model_selection.train_test_split(
            X, test_size=self.nq, random_state=1)


        with open(os.path.join(self.basedir, self.ds_fn), "wb") as f:
            np.array([self.nb, self.d], dtype='uint32').tofile(f)
            data.astype('float32').tofile(f)
        with open(os.path.join(self.basedir, self.qs_fn), "wb") as f:
            np.array([self.nq, self.d], dtype='uint32').tofile(f)
            queries.astype('float32').tofile(f)

        print("Computing groundtruth")

        nbrs = NearestNeighbors(n_neighbors=100, metric="euclidean", algorithm='brute').fit(data)
        D, I = nbrs.kneighbors(queries)
        with open(os.path.join(self.basedir, self.gt_fn), "wb") as f:
            np.array([self.nq, 100], dtype='uint32').tofile(f)
            I.astype('uint32').tofile(f)
            D.astype('float32').tofile(f)

    def search_type(self):
        return "knn"

    def distance(self):
        return "euclidean"

    def __str__(self):
        return f"Random({self.nb})"

    def default_count(self):
        return 10


DATASETS = {
    'bigann-1B': lambda : BigANNDataset(1000),
    'bigann-100M': lambda : BigANNDataset(100),
    'bigann-10M': lambda : BigANNDataset(10),

    'deep-1B': lambda : Deep1BDataset(),
    'deep-100M': lambda : Deep1BDataset(100),
    'deep-50M': lambda : Deep1BDataset(50),
    'deep-20M': lambda : Deep1BDataset(20),
    'deep-10M': lambda : Deep1BDataset(10),
    'deep-5M': lambda : Deep1BDataset(5),
    'deep-2M': lambda : Deep1BDataset(2),
    'deep-1M': lambda : Deep1BDataset(1),
    'deep-10K': lambda : Deep1BDataset(0.01),

    'ssnpp-1B': lambda : SSNPPDataset(1000),
    'ssnpp-10M': lambda : SSNPPDataset(10),
    'ssnpp-100M': lambda : SSNPPDataset(100),
    'ssnpp-1M': lambda : SSNPPDataset(1),

    'text2image-1B': lambda : Text2Image1B(),
    'text2image-1M': lambda : Text2Image1B(1),
    'text2image-10M': lambda : Text2Image1B(10),
    'text2image-100M': lambda : Text2Image1B(100),

    'msturing-1B': lambda : MSTuringANNS(1000),
    'msturing-1M': lambda : MSTuringANNS(1),
    'msturing-10M': lambda : MSTuringANNS(10),
    'msturing-100M': lambda : MSTuringANNS(100),

    'msspacev-1B': lambda : MSSPACEV1B(1000),
    'msspacev-10M': lambda : MSSPACEV1B(10),
    'msspacev-100M': lambda : MSSPACEV1B(100),
    'msspacev-1M': lambda : MSSPACEV1B(1),

    'random-xs': lambda : RandomDS(10000, 1000, 20),
    'random-s': lambda : RandomDS(100000, 1000, 50),

    'random-range-xs': lambda : RandomRangeDS(10000, 1000, 20),
    'random-range-s': lambda : RandomRangeDS(100000, 1000, 50),
}
