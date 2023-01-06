# any2dataset
[![pypi](https://img.shields.io/pypi/v/any2dataset.svg)](https://pypi.python.org/pypi/any2dataset)
[![Try it on gitpod](https://img.shields.io/badge/try-on%20gitpod-brightgreen.svg)](https://gitpod.io/#https://github.com/rom1504/any2dataset)
[![Chat on discord](https://img.shields.io/discord/823813159592001537?color=5865F2&logo=discord&logoColor=white)](https://discord.gg/eq3cAMZtCC)

Easily turn large sets of file urls to an file dataset.

Not ready for show time yet, see #1 and #2


## Install

pip install any2dataset

## Examples

Example of datasets to download with example commands are available in the [dataset_examples](dataset_examples) folder. In particular:
* [freesound](dataset_examples/freesound.md) 500k file/text pairs

## Usage

First get some file url list. For example:
```
echo 'https://freesound.org/apiv2/files/632625/download/' >> myfilelist.txt
echo 'https://freesound.org/apiv2/files/632619/download/' >> myfilelist.txt
```

Then, run the tool:

```
any2dataset --url_list=myfilelist.txt --output_folder=output_folder --thread_count=64 --file_size=256
```

The tool will then automatically download the urls, subsample them, and store them with that format:
* output_folder
    * 00000.tar containing:
        * 000000000.flat
        * 000000001.flat
        * 000000002.flat

with each number being the position in the list. The subfolders avoids having too many files in a single folder.

If **captions** are provided, they will be saved as 0.txt, 1.txt, ...

This can then easily be fed into machine learning training or any other use case.

Also a .parquet file will be saved with the same name as the subfolder/tar files containing these same metadata.
It can be used to analyze the results efficiently.

.json files will also be saved with the same name suffixed by _stats, they contain stats collected during downloading (download time, number of success, ...)

## Python examples

Checkout these examples to call this as a lib:
* [simple_example.py](examples/simple_example.py)

## API

This module exposes a single function `download` which takes the same arguments as the command line tool:

* **url_list** A file with the list of url of files to download. It can be a folder of such files. (*required*)
* **file_size** The size to subsample file to (default *256*)
* **output_folder** The path to the output folder. If existing subfolder are present, the tool will continue to the next number. (default *"files"*)
* **processes_count** The number of processes used for downloading the pictures. This is important to be high for performance. (default *1*)
* **thread_count** The number of threads used for downloading the pictures. This is important to be high for performance. (default *256*)
* **output_format** decides how to save pictures (default *files*)
  * **files** saves as a set of subfolder containing pictures
  * **webdataset** saves as tars containing pictures
  * **parquet** saves as parquet containing pictures as bytes
  * **tfrecord** saves as tfrecord containing pictures as bytes
  * **dummy** does not save. Useful for benchmarks
* **input_format** decides how to load the urls (default *txt*)
  * **txt** loads the urls as a text file of url, one per line
  * **csv** loads the urls and optional caption as a csv
  * **tsv** loads the urls and optional caption as a tsv
  * **tsv.gz** loads the urls and optional caption as a compressed (gzip) tsv.gz
  * **json** loads the urls and optional caption as a json
  * **parquet** loads the urls and optional caption as a parquet
* **url_col** the name of the url column for parquet and csv (default *url*)
* **caption_col** the name of the caption column for parquet and csv (default *None*)
* **number_sample_per_shard** the number of sample that will be downloaded in one shard (default *10000*)
* **save_additional_columns** list of additional columns to take from the csv/parquet files and save in metadata files (default *None*)
* **timeout** maximum time (in seconds) to wait when trying to download an file (default *10*)
* **enable_wandb** whether to enable wandb logging (default *False*)
* **wandb_project** name of W&B project used (default *any2dataset*)
* **oom_shard_count** the order of magnitude of the number of shards, used only to decide what zero padding to use to name the shard files (default *5*)
* **compute_md5** compute md5 of raw files and store it in metadata (default *True*)
* **distributor** choose how to distribute the downloading (default *multiprocessing*)
  * **multiprocessing** use a multiprocessing pool to spawn processes
  * **pyspark** use a pyspark session to create workers on a spark cluster (see details below)
* **subjob_size** the number of shards to download in each subjob supporting it, a subjob can be a pyspark job for example (default *1000*)
* **retries** number of time a download should be retried (default *0*)
* **disable_all_reencoding** if set to True, this will keep the file files in their original state with no resizing and no conversion, will not even check if the file is valid. Useful for benchmarks. To use only if you plan to post process the files by another program and you have plenty of storage available. (default *False*)

## Output format choice

any2dataset support several formats. There are trade off for which to choose:
* files: this is the simplest one, files are simply saved as files. It's good for up to 1M samples on a local file system. Beyond that performance issues appear very fast. Handling more than a million files in standard filesystem does not work well.
* webdataset: webdataset format saves samples in tar files, thanks to [webdataset](https://webdataset.github.io/webdataset/) library, this makes it possible to load the resulting dataset fast in both pytorch, tensorflow and jax. Choose this for most use cases. It works well for any filesystem
* parquet: parquet is a columnar format that allows fast filtering. It's particularly easy to read it using pyarrow and pyspark. Choose this if the rest of your data ecosystem is based on pyspark. [petastorm](https://github.com/uber/petastorm) can be used to read the data but it's not as easy to use as webdataset
* tfrecord: tfrecord is a protobuf based format. It's particularly easy to use from tensorflow and using [tf data](https://www.tensorflow.org/guide/data). Use this if you plan to use the dataset only in the tensorflow ecosystem. The tensorflow writer does not use fsspec and as a consequence supports only a limited amount of filesystem, including local, hdfs, s3 and gcs. It is also less efficient than the webdataset writer when writing to other filesystems than local, losing some 30% performance.

## File system support

Thanks to [fsspec](https://filesystem-spec.readthedocs.io/en/latest/), any2dataset supports reading and writing files in [many file systems](https://github.com/fsspec/filesystem_spec/blob/6233f315548b512ec379323f762b70764efeb92c/fsspec/registry.py#L87).
To use it, simply use the prefix of your filesystem before the path. For example `hdfs://`, `s3://`, `http://`, or `gcs://`.
Some of these file systems require installing an additional package (for example s3fs for s3, gcsfs for gcs).
See fsspec doc for all the details.

If you need specific configuration for your filesystem, you may handle this problem by using the [fsspec configuration system](https://filesystem-spec.readthedocs.io/en/latest/features.html#configuration) that makes it possible to create a file such as `.config/fsspec/s3.json` and have information in it such as:
```
{
  "s3": {
    "client_kwargs": {
            "endpoint_url": "https://some_endpoint",
            "aws_access_key_id": "your_user",
           "aws_secret_access_key": "your_password"
    }
  }
}
```
Which may be necessary if using s3 compatible file systems such as [minio](https://min.io/). That kind of configuration also work for all other fsspec-supported file systems.

## Distribution modes

any2dataset supports several distributors.
* multiprocessing which spawns a process pool and use these local processes for downloading
* pyspark which spawns workers in a spark pool to do the downloading

multiprocessing is a good option for downloading on one machine, and as such it is the default.
Pyspark lets any2dataset use many nodes, which makes it as fast as the number of machines.
It can be particularly useful if downloading datasets with more than a billion file.

### pyspark configuration

In order to use any2dataset with pyspark, you will need to do this:
1. `pip install pyspark`
2. use the `--distributor pyspark` option
3. tweak the `--subjob_size 1000` option: this is the number of files to download in each subjob. Increasing it will mean a longer time of preparation to put the feather files in the temporary dir, a shorter time will mean sending less shards at a time to the pyspark job.

By default a local spark session will be created.
You may want to create a custom spark session depending on your specific spark cluster.
To do that check [pyspark_example.py](examples/pyspark_example.py), there you can plug your custom code to create a spark session, then
run any2dataset which will use it for downloading.

To create a spark cluster check the [distributed any2dataset tutorial](examples/distributed_any2dataset_tutorial.md)

## Integration with Weights & Biases

To enable wandb, use the `--enable_wandb=True` option.

Performance metrics are monitored through [Weights & Biases](https://wandb.com/).

![W&B metrics](doc_assets/wandb_metrics.png)

In addition, most frequent errors are logged for easier debugging.

![W&B table](doc_assets/wandb_table.png)

Other features are available:

* logging of environment configuration (OS, python version, CPU count, Hostname, etc)
* monitoring of hardware resources (GPU/CPU, RAM, Disk, Networking, etc)
* custom graphs and reports
* comparison of runs (convenient when optimizing parameters such as number of threads/cpus)

When running the script for the first time, you can decide to either associate your metrics to your account or log them anonymously.

You can also log in (or create an account) before by running `wandb login`.

## For development

Either locally, or in [gitpod](https://gitpod.io/#https://github.com/rom1504/any2dataset) (do `export PIP_USER=false` there)

Setup a virtualenv:

```
python3 -m venv .env
source .env/bin/activate
pip install -e .
```

to run tests:
```
pip install -r requirements-test.txt
```
then 
```
make lint
make test
```

You can use `make black` to reformat the code

`python -m pytest -x -s -v tests -k "dummy"` to run a specific test

## Benchmarks

### 10000 file benchmark

```
cd tests/test_files
bash benchmark.sh
```

