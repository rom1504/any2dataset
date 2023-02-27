import webdataset as wds
import torch
import json
import librosa
import numpy as np
import random
import time

def preprocess(sample:tuple, target_sr:int=48000, seq_len:int=5):
    '''
    Resamples audio to target_sr and selects random seq_len seconds segment from audio
    if audio is shorter than seq_len, repeats audio k times (k=seq_len/audio_len, where audio_len lenght of audio)
    Converts all audio samples to mono format
    Converts captions from JSON to string:
        if there's audio meta tags like title, artist, genre constructs caption playing {genre} song "{title}" by {artist}
        uses raw cpation otherwise
    '''
    audio, json_data = sample
    json_data = json.loads(json_data.decode())
   
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

                label = f'playing {genre} song "{title}" {artist}'
            except:
                pass
    audio, sr = audio
    s = time.time()
    audio_len = librosa.get_duration(y=audio, sr=sr)
    ac, samples_total = audio.shape
    seq_len *= sr
    if samples_total >= seq_len:
        starts = samples_total - seq_len
        start = random.randrange(0, starts)
        end = start + seq_len
        audio_segment = audio[:, start:end]
    else:
        audio_segment = np.repeat(np.array(audio), seq_len//sr//audio_len+1)[:seq_len]
        
        try:
            audio_segment = audio_segment.reshape(ac, seq_len)
        except:
            audio_segment = audio_segment.reshape(1, seq_len)

    
    audio_segment = np.array(audio_segment)

    if ac == 2:
        audio_segment = librosa.to_mono(audio_segment).reshape(1, seq_len)

    audio_segment = librosa.resample(y=audio_segment, orig_sr=sr, target_sr=target_sr)

    audio_segment = audio_segment[:, :target_sr*seq_len//sr]

    label = f'{json_data["caption"]}'
    

    return {'flac': audio_segment, 'json': {'label': label}}

def get_dataset(urls: list):
    '''
    Pass s3 urls and get processed torch dataset
    '''
    urls = [f'pipe:aws s3 cp {url} -' for url in urls]
    dataset = (
           wds.WebDataset(urls)
           .decode(wds.torch_audio)
           .to_tuple("flac", "json")
           .map(preprocess)
    )
    return dataset

urls = ['s3://...']
dataset = get_dataset(urls)

batch_size = 30

loader = torch.utils.data.DataLoader(dataset, batch_size=batch_size)

# testing

for i, batch in enumerate(loader):
    aud, d = batch
