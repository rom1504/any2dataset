"""subsampler module handle file resizing"""

import subprocess as sp
import io
import shlex
import json


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
            meta = None

            cmd = [
                '/fsx/iejmac/ffmpeg2/ffmpeg',
                '-v', 'error',
                '-i', 'pipe:',
                '-f', self.f,
                'pipe:'
                ]

            proc = sp.Popen(cmd, stdout=sp.PIPE, stdin=sp.PIPE, stderr=sp.PIPE)
            out, err = proc.communicate(file_stream.read())
            err = err.decode()
            proc.wait()
            if err != '':
                return None, None, err
            file_stream = out
            if self.get_meta:

                try:

                    cmd = '/fsx/iejmac/ffmpeg2/ffprobe -v error -show_format -of json pipe:'.split()

                    proc = sp.Popen(cmd, stdout=sp.PIPE, stdin=sp.PIPE, stderr=sp.PIPE)
                    meta, err = proc.communicate(file_stream)
                    err = err.decode()
                    if err != '':
                        return files_tream, None, err


                    meta = json.loads(meta)['format']['tags']
                except Exception as err:
                    meta = None

            return file_stream, meta, None

        except Exception as err:  # pylint: disable=broad-except
            return None, None, str(err)
