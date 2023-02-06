import time
import json
import requests

def read_bearer_token(file_path = 'bearer_token.txt'):
    '''Reads in bearer tokens from token.txt'''
    with open(file_path, "r") as f:
        for line in f:
            BEARER_TOKEN = line.strip()
            break
    f.close()
    return BEARER_TOKEN

DATA_FOLDER = 'json'
BEARER_TOKEN = read_bearer_token()
HASHTAGS = []

# read hashtag queries from queries.txt
with open('queries.txt', 'r') as f:
    for line in f:
        HASHTAGS.append(line.strip())

PARAMS = {
'max_results': "500",
'start_time': "2010-11-06T00:00:00Z", # (YYYY-MM-DDTHH:mm:ssZ) -> RFC3339 date-time
'end_time': "2023-01-31T23:59:59Z", # (YYYY-MM-DDTHH:mm:ssZ)
'tweet.fields': "attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld",
'expansions': "attachments.poll_ids,attachments.media_keys,author_id,geo.place_id,in_reply_to_user_id,referenced_tweets.id,entities.mentions.username,referenced_tweets.id.author_id",
'media.fields': "duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics,alt_text",
'place.fields': "contained_within,country,country_code,full_name,geo,id,name,place_type",
'poll.fields': "duration_minutes,end_datetime,id,options,voting_status",
'user.fields': "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld",
}

for h in HASHTAGS:
    print(f'Downloading {h}...')
    count = 0
    PARAMS['query'] = h
    last_download_failed = False
    try:
        del PARAMS['next_token']
    except KeyError:
        pass
    # API pagination
    while 'next_token' in PARAMS or count == 0 or last_download_failed:
        time.sleep(3)
        try:
            print(f'Downloading {h} with next token {PARAMS["next_token"]}...')
        except KeyError:
            pass
        s = requests.Session()
        s.headers.update({'Authorization': f'Bearer {BEARER_TOKEN}'})
        URL = f"https://api.twitter.com/2/tweets/search/all"
        req = requests.models.PreparedRequest()
        req.prepare_url(URL, PARAMS)
        r = s.get(req.url)
        if not r.ok:
            last_download_failed = True
            with open('download_log.txt', 'a') as f:
                f.write(f'Download failed for {h} -- status not ok -- {r.status_code} and {r.content}\n')
                continue
        try:
            j = json.loads(r.content)
        except:
            last_download_failed = True
            with open('download_log.txt', 'a') as f:
                f.write(f'Download failed for {h} -- JSON content not correctly loaded\n')
                continue
        try:
            PARAMS['next_token'] = j['meta']['next_token']
        except KeyError:
            try:
                if not last_download_failed:
                    del PARAMS['next_token']
            except KeyError:
                pass
        if not last_download_failed:
            count += 1
        try:
            if not j['meta']['result_count'] == 0:
                with open(f'{DATA_FOLDER}/{h}_{count}.json', 'w') as f:
                    json.dump(j, f, ensure_ascii=False)
                last_download_failed = False
        except:
            last_download_failed = True
            with open('download_log.txt', 'a') as f:
                f.write(f'Download failed for {h} -- JSON export failed\n')
                continue

