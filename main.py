import pandas as pd
import time
import json
import requests

from utils import *

BEARER_TOKEN = read_bearer_token()

# Run once:
#res = get_id_dict()

uids = get_most_recent_ids()
done_uids = get_done_uids()

for i, uid in enumerate(uids):
    if str(uid) in done_uids:
        continue
    get_most_recent_tweets_account(uid, BEARER_TOKEN, PARAMS, 
                                   verbose=True, save_file=True)
    done_uids.append(str(uid))
    if i%15 == 0 and i>1:
        export_done_uids(done_uids)
    print(done_uids)

export_done_uids(done_uids)
