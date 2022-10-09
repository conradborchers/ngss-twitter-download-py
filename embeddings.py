"""
Get sentence embeddings for all tweets.
"""
import glob
import json
import time

import pandas as pd

from tqdm import tqdm

from sentence_transformers import SentenceTransformer
model = SentenceTransformer('paraphrase-MiniLM-L6-v2', device='cpu')

# Save progress so that embeddings must not be generated in one session
EMBEDDINGS_PATH = './embeddings.json'

def load_embeddings(f=EMBEDDINGS_PATH):
    with open(f, 'r') as handle:
        d_in = json.load(handle)
    return d_in

def save_embeddings(d_out, f=EMBEDDINGS_PATH):
    with open(f, 'w') as handle:
        json.dump(d_out, handle, ensure_ascii = False)
    return

# id -> text
d_tweet = dict()

try:
    d_emb = load_embeddings() 
except:
    d_emb = dict() # If there is no file in ./
    save_embeddings(d_emb)

# Read texts
files = glob.glob('json/*.json')
for file in tqdm(files):
    with open(file) as f:
        d = json.load(f)
    for tweet in d['data']:
        d_tweet[tweet['id']] = tweet['text']

# Encode texts, make breaks to not strain CPU
count = 0
for key in tqdm(d_tweet.keys()):
    # Every 10k encodings
    #if count//10000:
    #    save_embeddings(d_emb)
    #    time.sleep(60*3) # sleep 3 mins
    if key not in d_emb:
        d_emb[key] = model.encode(d_tweet[key]).tolist()
        count += 1

# Create DF
rows = []
for key in tqdm(d_tweet.keys()):
    if key not in d_emb:
        continue
    rows.append(pd.DataFrame([key] + d_emb[key]).T)

# Export
out = pd.concat(rows)  
out.columns = ['status_id'] + ['text_emb_dim_' + str(i) for i in range(1, out.shape[1])]
out.to_csv('./sentence-embeddings-ngss.csv', index=False)
