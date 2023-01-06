from any2dataset import download
import shutil
import os

output_dir = os.path.abspath("bench")

if os.path.exists(output_dir):
    shutil.rmtree(output_dir)

download(
    processes_count=16,
    thread_count=32,
    url_list="../tests/test_files/test_10000.parquet",
    output_folder=output_dir,
    output_format="webdataset",
    input_format="parquet",
    url_col="download_url",
    caption_col="description",
    enable_wandb=True,
    number_sample_per_shard=1000,
    distributor="multiprocessing",
)

# rm -rf bench
