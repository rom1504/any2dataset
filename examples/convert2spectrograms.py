from riffusion.spectrogram_image_converter import SpectrogramImageConverter
from riffusion.spectrogram_params import SpectrogramParams
from pydub import AudioSegment
import time
from PIL import Image
import numpy as np
import glob
import librosa
import io
import webdataset as wds
import torch
import json


def get_dataset(urls: list):
    '''
    Pass s3 urls and get torch dataset
    '''
    urls = [f'pipe:aws s3 cp {url} -' for url in urls]
    dataset = (
           wds.WebDataset(urls)
           .to_tuple("flac", "json")
    )
    return dataset


def write_wds(src, output, maxcount=999999999, seq_len=10, target_sr = 44100):
    '''
    Split every audio into seq_len seconds chunks, resample to target_sr and convert into spectrogram
    '''
    params = SpectrogramParams()
    converter = SpectrogramImageConverter(params)
    key = 0
    with wds.TarWriter(output) as dst:
        for audio, data in islice(src, 0, maxcount):
            data = json.loads(data.decode())
            audio = AudioSegment.from_file(io.BytesIO(audio))
            audio = audio.set_frame_rate(target_sr)
            dur = audio.duration_seconds
            for j in range(0, int(dur)-seq_len, seq_len):


                data['start'] = j
                data['duration'] = dur
                
                audio_segment = audio[j*1000: (j+seq_len)*1000]
                image = converter.spectrogram_image_from_audio(audio_segment)

                sample = {
                    "__key__": f'{key:05}',
                    "jpg": image,
                    "json": data
                }
                dst.write(sample)
                key += 1

dataset = get_dataset(urls)
write_wds(dataset, 'test.tar')