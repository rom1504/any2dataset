from any2dataset import download
import time
import sys
from pyspark.sql import SparkSession

output_dir = 's3://s-laion/CC_AUDIO_WAT_WDS'

if __name__ == '__main__':

    s = time.time()

    os.environ["PYSPARK_PYTHON"] = sys.executable
    os.environ["PYSPARK_DRIVER_PYTHON"] = sys.executable

    spark = (
        SparkSession.builder
        .config("spark.submit.deployMode", "client") \
        .config("spark.executor.memory", "16GB")
        .config("spark.executor.memoryOverhead", "8GB")
        .config("spark.task.maxFailures", "100")
        .config("spark.ui.port", "5041")
        .master("spark://cpu128-dy-c6i-32xlarge-1:7077")
        .appName("spark-stats")
        .getOrCreate()
    )

    download(
        processes_count=128,
        thread_count=48,
        url_list="NON_TOXIC_AUDIO",
        output_folder=output_dir,
        output_format="webdataset",
        input_format="parquet",
        url_col="url",
        caption_col="alt",
        enable_wandb=True,
        number_sample_per_shard=100,
        distributor="pyspark",
        get_meta = True
    )

    e = time.time()
    print(e - s)