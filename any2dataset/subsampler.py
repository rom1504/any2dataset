"""subsampler module handle file resizing"""

import subprocess as sp
import io
import shlex
import json
import sys
import tempfile
import uuid



class Subsampler:
    """
    subsample files
    Expose a __call__ method to be used as a callable object

    Should be used to subsample one file at a time

    """

    def __init__(
        self,
        disable_all_reencoding=False,
        f='flac',
        get_meta=False
    ):
        self.disable_all_reencoding = disable_all_reencoding
        self.get_meta = get_meta
        self.f = f

    def __call__(self, file_stream):
        """
        input: an file stream
        output: file_stream, err
        """
        try:

            file_bytes = file_stream.read()

            meta = dict()

            with tempfile.TemporaryDirectory() as temp_dir:

                tmp_name = f'{temp_dir}/{uuid.uuid4()}.{self.f}'
            

                cmd = [
                    '/fsx/iejmac/ffmpeg2/ffmpeg',
                    '-v', 'error',
                    '-i', 'pipe:',
                    '-f', self.f,
                    tmp_name
                    ]

                proc = sp.Popen(cmd, stdin=sp.PIPE, stderr=sp.PIPE)
                out, err = proc.communicate(file_bytes)
                err = err.decode()
                proc.wait()
                if err != '':
                    return None, None, err

                if self.get_meta:

                    try:
                        cmd = f'/fsx/iejmac/ffmpeg2/ffprobe -v error -show_format -of json {tmp_name}'.split()

                        proc = sp.Popen(cmd, stderr=sp.PIPE, stdout=sp.PIPE)
                        meta, err = proc.communicate()
                        meta_err = err.decode()

                        meta = json.loads(meta)
                        bytes_size = sys.getsizeof(file_bytes)*8
                        bit_rate = meta['format']['bit_rate']
                        duration = bytes_size/int(bit_rate)
                        meta = meta['format']
                        # if 'tags' in meta.keys():
                        #     meta = meta['tags']
                        # else:
                        #     meta = dict()
                        # meta['duration'] = duration
                        # meta['size'] = bytes_size
                        # meta['bit_rate'] = bit_rate
                    except Exception as err:
                        # print(err)
                        meta = {}

                with open(tmp_name, 'rb') as f:
                    file_stream = f.read()

                return file_stream, meta, None

        except Exception as err:  # pylint: disable=broad-except
            return None, None, str(err)
