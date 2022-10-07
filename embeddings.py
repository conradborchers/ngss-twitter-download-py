"""
Get sentence embeddings for all tweets.
"""
import glob
import json
import time
from tqdm import tqdm

from sentence_transformers import SentenceTransformer
model = SentenceTransformer('paraphrase-MiniLM-L6-v2', device='cpu')

# id -> text; embedding
d_tweet, d_emb = dict(), dict()

# Read texts
files = glob.glob('json/*.json')
for file in tqdm(files):
    with open(file) as f:
        d = json.load(f)
    for tweet in d['data']:
        d_tweet[tweet['id']] = tweet['text']

# Encode texts, make breaks to not strain CPU
for i, key in tqdm(enumerate(d_tweet.keys())):
    if i///10000:
        time.sleep(60*5) # sleep 5 mins
    d_emb[key] = model.encode(d_tweet[key])

