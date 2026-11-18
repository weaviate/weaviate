#!/usr/bin/env python3
"""Drop the system's page cache of every file under a directory, on Linux,
without root: posix_fadvise(POSIX_FADV_DONTNEED) on each. The kernel only
drops clean pages, so the files are synced first. The benchmarks of v4.0 run
it with their index closed, before every cold batch.

usage: drop_cache.py <dir>
"""
import os
import sys


def main(root):
    if not hasattr(os, 'posix_fadvise'):
        sys.exit('posix_fadvise is not available on this system: give the benchmark another drop command')
    files = 0
    for folder, _, names in os.walk(root):
        for name in names:
            path = os.path.join(folder, name)
            if not os.path.isfile(path) or os.path.islink(path):
                continue
            fd = os.open(path, os.O_RDONLY)
            try:
                os.fsync(fd)
                os.posix_fadvise(fd, 0, 0, os.POSIX_FADV_DONTNEED)
            finally:
                os.close(fd)
            files += 1
    if files == 0:
        sys.exit('no file under %s' % root)


if __name__ == '__main__':
    main(sys.argv[1])
