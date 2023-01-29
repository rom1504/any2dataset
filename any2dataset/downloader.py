"""the downloader module handles the downloading"""

from multiprocessing.pool import ThreadPool
from threading import Semaphore
import urllib.request
import io
import math
import time
import hashlib
import pyarrow as pa
import traceback
import subprocess
import soundfile as sf

import fsspec
from .logger import CappedCounter
from .logger import write_stats

def download_file(row, timeout):
    """Download a file with ffmpeg"""
    key, url = row
    file_stream = None
    try:
        cmd = f'ffmpeg -v error -i {url} -timeout {timeout}  -c copy -f s16le pipe:1'
        p = subprocess.Popen(cmd.split(), stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        file_stream = io.BytesIO(p.stdout.read())
        ffmpeg_err = p.stderr.read().decode('utf-8')
        if ffmpeg_err != '':
            return key, None, ffmpeg_err
        return key, file_stream, None
    except Exception as err:  # pylint: disable=broad-except
        if file_stream is not None:
            file_stream.close()
        return key, None, str(err)


def download_file(row, timeout):
    """Download a file with urllib"""
    key, url = row
    file_stream = None
    try:
        request = urllib.request.Request(
            url,
            data=None,
            headers={"User-Agent": "Mozilla/5.0 (X11; Ubuntu; Linux x86_64; rv:72.0) Gecko/20100101 Firefox/72.0"},
        )
        with urllib.request.urlopen(request, timeout=timeout) as r:
            file_stream = io.BytesIO(r.read())
            # sf.read(file_stream)
        return key, file_stream, None
    except Exception as err:  # pylint: disable=broad-except
        if file_stream is not None:
            file_stream.close()
        return key, None, str(err)


def download_file_with_retry(row, timeout, retries):
    for _ in range(retries + 1):
        key, file_stream, err = download_file(row, timeout)
        if file_stream is not None:
            return key, file_stream, err
    return key, None, err


def compute_key(key, shard_id, oom_sample_per_shard, oom_shard_count):
    true_key = (10**oom_sample_per_shard) * shard_id + key
    key_format = oom_sample_per_shard + oom_shard_count
    str_key = "{true_key:0{key_format}d}".format(  # pylint: disable=consider-using-f-string
        key_format=key_format, true_key=true_key
    )
    return str_key


class Downloader:
    """The downloader class gets calls with shards, download them then call the writer to write them down"""

    def __init__(
        self,
        sample_writer_class,
        subsampler,
        thread_count,
        save_caption,
        output_folder,
        column_list,
        timeout,
        number_sample_per_shard,
        oom_shard_count,
        compute_md5,
        retries,
    ) -> None:
        self.sample_writer_class = sample_writer_class
        self.subsampler = subsampler
        self.thread_count = thread_count
        self.save_caption = save_caption
        self.output_folder = output_folder
        self.column_list = column_list
        self.timeout = timeout
        self.number_sample_per_shard = number_sample_per_shard
        self.oom_shard_count = oom_shard_count
        self.compute_md5 = compute_md5
        self.retries = retries

    def __call__(
        self,
        row,
    ):
        try:
            return self.download_shard(row)
        except Exception as err:  # pylint: disable=broad-except
            traceback.print_exc()
            print(f"shard {row[0]} failed with error {err}")
            return (False, 0, 0, 0, 0, 0, None)

    def download_shard(
        self,
        row,
    ):
        """Function to start an file downloading in one process"""

        shard_id, shard_file = row
        start_time = time.time()

        fs, shard_path = fsspec.core.url_to_fs(shard_file)
        with fs.open(shard_path, "rb") as f:
            df = pa.ipc.open_file(f).read_all()
        schema = df.schema
        schema = (
            schema.append(pa.field("key", pa.string()))
            .append(pa.field("status", pa.string()))
            .append(pa.field("error_message", pa.string()))
        )

        if self.compute_md5:
            schema = schema.append(pa.field("md5", pa.string()))

        pydict = df.select(self.column_list).to_pydict()
        shard_to_dl = list(enumerate(zip(*(pydict[col] for col in self.column_list))))
        del pydict
        del df

        status_dict = CappedCounter()

        count = len(shard_to_dl)
        successes = 0
        failed_to_download = 0
        failed_to_subsample = 0
        url_indice = self.column_list.index("url")
        caption_indice = self.column_list.index("caption") if "caption" in self.column_list else None
        key_url_list = [(key, x[url_indice]) for key, x in shard_to_dl]

        # this prevents an accumulation of more than twice the number of threads in sample ready to subsample
        # limit the memory usage
        semaphore = Semaphore(self.thread_count * 2)

        def data_generator():
            for e in key_url_list:
                semaphore.acquire()  # pylint: disable=consider-using-with
                yield e

        loader = data_generator()

        # give schema to writer
        sample_writer = self.sample_writer_class(
            shard_id,
            self.output_folder,
            self.save_caption,
            self.oom_shard_count,
            schema,
        )
        oom_sample_per_shard = math.ceil(math.log10(self.number_sample_per_shard))
        with ThreadPool(self.thread_count) as thread_pool:
            for key, file_stream, error_message in thread_pool.imap_unordered(
                lambda x: download_file_with_retry(x, timeout=self.timeout, retries=self.retries),
                loader,
            ):
                try:
                    _, sample_data = shard_to_dl[key]
                    str_key = compute_key(key, shard_id, oom_sample_per_shard, self.oom_shard_count)
                    meta = {
                        **{self.column_list[i]: sample_data[i] for i in range(len(self.column_list))},
                        "key": str_key,
                        "status": None,
                        "error_message": error_message,
                    }
                    if self.compute_md5:
                        meta["md5"] = None
                    if error_message is not None:
                        failed_to_download += 1
                        status = "failed_to_download"
                        status_dict.increment(error_message)
                        meta["status"] = status
                        sample_writer.write(
                            None,
                            str_key,
                            sample_data[caption_indice] if caption_indice is not None else None,
                            meta,
                        )
                        semaphore.release()
                        continue
                    file_stream.seek(0)
                    (
                        file,
                        error_message,
                    ) = self.subsampler(file_stream)
                    if error_message is not None:
                        failed_to_subsample += 1
                        status = "failed_to_subsample"
                        status_dict.increment(error_message)
                        meta["status"] = status
                        meta["error_message"] = error_message
                        sample_writer.write(
                            None,
                            str_key,
                            sample_data[caption_indice] if caption_indice is not None else None,
                            meta,
                        )
                        file_stream.close()
                        del file_stream
                        semaphore.release()
                        continue
                    successes += 1
                    status = "success"
                    status_dict.increment(status)

                    if self.compute_md5:
                        file_stream.seek(0)
                        meta["md5"] = hashlib.md5(file_stream.read()).hexdigest()

                    meta["status"] = status
                    file_stream.close()
                    del file_stream

                    sample_writer.write(
                        file,
                        str_key,
                        sample_data[caption_indice] if caption_indice is not None else None,
                        meta,
                    )
                except Exception as err:  # pylint: disable=broad-except
                    traceback.print_exc()
                    print(f"Sample {key} failed to download: {err}")
                semaphore.release()

            sample_writer.close()
            thread_pool.terminate()
            thread_pool.join()
            del thread_pool

        end_time = time.time()
        write_stats(
            self.output_folder,
            shard_id,
            count,
            successes,
            failed_to_download,
            failed_to_subsample,
            start_time,
            end_time,
            status_dict,
            self.oom_shard_count,
        )
        fs.rm(shard_path)
