# naive-bench

[![Latest Release](http://github-release-version.herokuapp.com/github/onedata/naive-bench/release.svg)](https://github.com/onedata/naive-bench/releases/latest)
[![Build Status](https://travis-ci.org/onedata/naive-bench.svg?branch=master)](https://travis-ci.org/onedata/naive-bench)
[![Codacy Badge](https://api.codacy.com/project/badge/Grade/b008f8665fed427291a26d25c2fc9433)](https://www.codacy.com/app/groundnuty/naive-bench_2?utm_source=github.com&amp;utm_medium=referral&amp;utm_content=onedata/naive-bench&amp;utm_campaign=Badge_Grade)
[![Latest Docker Tag](https://images.microbadger.com/badges/image/onedata/naive-bench.svg)](https://microbadger.com/images/onedata/naive-bench "Get your own image badge on microbadger.com")
[![Latest Docker Tag Details](https://images.microbadger.com/badges/version/onedata/naive-bench.svg)](https://microbadger.com/images/onedata/naive-bench "Get your own version badge on microbadger.com")

Simple filesystem performance benchmark, creates X files of size Y, overwrites their content, performs linear read of all files, performs random read (using seek) of all files and finally removes all files.

The benchmark is not focused on throughput primarly, but rather on actual performance as can be experienced by users including possible overhead from file creation and seek operations. The tests can be run in parallel using a specified number of subprocesses ([multiprocessing](https://docs.python.org/3.6/library/multiprocessing.html) library)

Files are filled with random binary data pregenerated before running the tests.

All steps are timed and returned in CSV format, during runtime progress is printed continously to stderr.

All temporary files are created in the current directory in subfolder `naive-bench-data`.

## Requirements

* Python 3
* humanize package


## Usage

```
$ ./naive-bench.py

Usage: naive-bench.py [options]

Options:
  -h, --help            show this help message and exit
  -f FILECOUNT, --filecount=FILECOUNT
                        Number of files to create
  -s FILESIZE, --filesize=FILESIZE
                        Average created file size. The file sizes will be
                        random in the range (0.5*filesize, 1.5*filesize].
  -b BLOCKSIZE, --blocksize=BLOCKSIZE
                        Size of data block for random read test.
  -n NAME, --name=NAME  Name of storage which identifies the performed test.
                        Defaults to hostname.
  -c, --csv             Generate CSV output.
  -H, --no-header       Skip CSV header.
  -r, --read-only       This test will only perform read tests. It assumes
                        that the current folder contains 'naive-bench-data'
                        folder with test files uniformly numbered in the
                        specified range.
  -w, --write-only      This test will only perform write tests. This option
                        can be used to create data on storage for peforming
                        remote read tests.
  -k, --keep            Keep the files after running the test.
  -u, --truncate-after-create
                        Truncates the created files to 0 size so that
                        consecutive write benchmarks work with empty files.
  -F, --force           Run the test even when the available storage size is
                        too small.
  -d DEVIATION, --deviation=DEVIATION
                        Generate the files with random size in range
                        ((1.0-deviation)*filesize, (1.0+deviation)*filesize].
  -t THREADCOUNT, --thread-count=THREADCOUNT
                        Number of threads to execute for each test.
  -P, --no-purge        If specified, disables cache clearing between steps.
```

## Examples

Create 100 1GB files by writing and reading blocks of 10MB using 10 processes.
```bash
./naive-bench.py --filecount 100 --filesize 1GB --blocksize 10MB  -t 10
```
