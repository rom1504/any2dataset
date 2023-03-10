import torch
import webdataset as wds
import sys
import os
from torchvision import transforms
from PIL import Image
import numpy as np
import time
import multiprocessing as mp


transform = transforms.Compose([
    transforms.ToTensor()
])


def preprocess(sample:tuple, target_sr:int=48000, seq_len:int=5):
    '''
    Resamples audio to target_sr and selects random seq_len seconds segment from audio
    if audio is shorter than seq_len, repeats audio k times (k=seq_len/audio_len, where audio_len lenght of audio)
    Converts all audio samples to mono format
    Converts captions from JSON to string:
        if there's audio meta tags like title, artist, genre constructs caption playing {genre} song "{title}" by {artist}
        uses raw cpation otherwise
    '''
    image, json_data = sample
    # json_data = json.loads(json_data.decode())
   
    audio_meta = json_data.get('audio_meta', None)
    
    if audio_meta is not None:
        tags = audio_meta.get('tags', None)
        if tags is not None:
            try:
                title, artist, genre = '', '', ''
                for k in tags.keys():
                    if k in ['title', 'TITLE']:
                        title = f'titled {tags[k]}'
                    if k in ['artist', 'ARTIST']:
                        artist = f'by {tags[k]}'
                    if k in ['genre', 'GENRE']:
                        genre = tags[k]

                label = f'{genre} song "{title}" {artist}'
            except:
                pass
    label = f'{json_data["caption"]}'

    return image, {'label': label}

def get_dataset(urls: list):
    '''
    Pass s3 urls and get processed torch dataset
    '''
    urls = [f'pipe:aws s3 cp {url} -' for url in urls]
    dataset = (
           wds.WebDataset(urls)
           .decode("pil")
           .to_tuple("jpg", "json")
           .map_tuple(transform)
           .map(preprocess)
    )
    return dataset

urls = [f's3://s-laion/CC_AUDIO_spectrograms/{i:05d}.tar' for i in range(10)]
dataset = get_dataset(urls)

batch_size = 32
n_workers = 16
loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size, num_workers=n_workers)

s = time.time()


for i, batch in enumerate(loader):
    im, label = batch

e = time.time()
print(f'Finished in {e-s} sec, num workers = {n_workers}, batch size = {batch_size}')
