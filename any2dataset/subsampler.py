"""subsampler module handle file resizing"""


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

    def __call__(self, file_stream):
        """
        input: an file stream
        output: file_str, width, height, original_width, original_height, err
        """
        try:
            if self.disable_all_reencoding:
                return file_stream.read(), None

            return file_stream.read(), None

        except Exception as err:  # pylint: disable=broad-except
            return None, str(err)
