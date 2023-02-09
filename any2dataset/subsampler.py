"""subsampler module handle file resizing"""

import subprocess as sp
import io


class Subsampler:
    """
    subsample files
    Expose a __call__ method to be used as a callable object

    Should be used to subsample one file at a time

    """

    def __init__(
        self,
        disable_all_reencoding=False,
    ):
        self.disable_all_reencoding = disable_all_reencoding

    def __call__(self, file_stream, f='flac'):
        """
        input: an file stream
        output: file_stream, err
        """
        try:

            cmd = [
                'ffmpeg',
                '-v', 'error',
                '-i', 'pipe:',
                '-f', f,
                'pipe:'
                ]

            proc = sp.Popen(cmd, stdout=sp.PIPE, stdin=sp.PIPE)
            file_stream = proc.communicate(file_stream.read())[0]
            proc.wait()

            return file_stream, None

        except Exception as err:  # pylint: disable=broad-except
            return None, str(err)
