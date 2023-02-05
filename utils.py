import json
import re
import requests
import datetime
import warnings
import time
import glob
import pandas as pd

class ApiError(Exception):
    """
    This is an empty class to raise custom exceptions 
    during API calls.
    """
    pass

def clean_link(s):
    uid = s.lower().split('twitter.com/')[-1].split('/')[0] 
    return uid

def get_url_ids(f='12-2022-twitter-links.csv'):
    df = pd.read_csv(f)
    uids = list(set(df.scraped_links.map(clean_link)))
    return uids

def get_most_recent_dict():
    tmp = pd.read_csv(max(glob.glob('twitter-ids-*')))
    return {user: uid for user, uid in zip(tmp.user, tmp.uid)}

def get_most_recent_ids():
    tmp = pd.read_csv(max(glob.glob('twitter-ids-*')))
    return sorted(list(set(tmp.uid.to_list())))

def get_id_dict():
    uids = get_url_ids()
    res = get_most_recent_dict() # OR: # res = dict()
    uids = list(set(uids) - set(res.keys()))
    for i, uid in enumerate(uids):
        res[uid] = look_up_twitter_acount_id(uid)
        if i%500 == 0 and i>0:
            print('exporting')
            export_id_dict(res)
    export_id_dict(res)
    return res

def export_id_dict(res):
    res = {k: v for k, v in res.items() if k!='' and v is not None}
    dd = pd.DataFrame.from_dict(res, orient='index').reset_index()
    dd.columns = ['user', 'uid']
    dd.to_csv(f'twitter-ids-{datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}.csv', index=False)
    return

def get_done_uids():
    if len(glob.glob('twitter-done-uids-*')) == 0:
        return []
    tmp = pd.read_csv(max(glob.glob('twitter-done-uids-*')))
    ans = sorted(list(set(tmp.uid.to_list())))
    return [str(i) for i in ans]

def export_done_uids(l: list):
    dd = pd.DataFrame({'uid': l})
    dd.to_csv(f'twitter-done-uids-{datetime.datetime.now().strftime("%Y-%m-%d-%H:%M:%S")}.csv', index=False)
    return

def read_bearer_token(file_path = 'bearer_token.txt'):
    """
    This is a helper function that reads a bearer token form
    a specified file path into a string object.
    
    Parameters
    ----------
    file_path : str
        Readable file path containing a document which includes
        the Twitter API bearer token in its first line. Defaults
        to 'bearer_token.txt'.
        
    Returns
    -------
    str
        The Twitter API bearer token as a string object.
    """
    # Will raise error if path does not exist
    with open(file_path, "r") as f:
        for line in f:
            BEARER_TOKEN = line.strip()
            break
    f.close() 
    return BEARER_TOKEN

def look_up_twitter_acount_id(user_name):
    """
    This is a helper function to set a simple Twitter API 
    request to look up a Twitter user ID based on a user
    handle (e.g., '@twitter' or 'nhsuk'). The user can both
    parse in the username as a handle (string including '@') 
    and as a string without the '@'. The Twitter user ID
    is needed for subsequent Twitter API calls.
    
    GET /2/users/by/username/:username
    
    App rate limit: 300 requests per 15-minute window
    
    https://developer.twitter.com/en/docs/twitter-api/users/lookup/api-reference/get-users-by-username-username
    
    Parameters
    ----------
    BEARER_TOKEN : str
        A Twitter API bearer token as a string object.
    user_name : str
        The username to look up (either including or 
        excluding) the '@' symbol.
        
    Returns
    -------
    str
        The Twitter user ID as a string object.
        
    Raises
    ------
    ApiError
        Either the request status code was not 200
        or the requested user name was malformed.
    """
    if not isinstance(user_name, str):
        return
    if len(user_name) == 0:
        return

    # If a handle was parsed, convert to user name
    if (user_name[0] == "@"):
        user_name = user_name[1:]
    
    # If user name does not only contain letters, numbers, or
    # underscores, raise error
    if not re.match("^[\w\d_]*$", user_name):
        #raise ApiError('The user name you requested seems to be malformed.')
        return
    
    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {BEARER_TOKEN}'})

    req = s.get(f'https://api.twitter.com/2/users/by?usernames={user_name}')
    time.sleep(2.5) # rate limit
    
    if req.status_code != 200:
        #raise ApiError(f'There was an error sending the request. '\
        #               f'Status code {req.status_code}')
        return
    
    page = json.loads(req.content)

    if 'data' not in page:
        return

    twitter_id = page['data'][0]['id']
    
    return twitter_id

def get_most_recent_tweets_account(ACCOUNT_ID, BEARER_TOKEN, PARAMS, 
                                   verbose=True, save_file=True):
    """
    This function is a download routine to get the most recent 
    Tweets of a specified Twitter account. It extracts the pagination 
    from the results of the API calls until either less than 100 
    results are returned or 32 API calls have been made (which is 
    the maximum number of tweets Twitter allows academic researchers 
    to download as of November 2021). The function uses the 
    GET /2/users/:id/tweets endpoint of the Twitter V2 API. The 
    endpoint allows for 900 requests per 15-minute window. Hence, 
    the thread pauses for 1 second in between API calls. More 
    information on the endpoint can be found here:
    https://developer.twitter.com/en/docs/twitter-api/tweets/timelines/api-reference/get-users-id-tweets
    
    Parameters
    ----------
    ACCOUNT_ID : str
        A Twitter user ID.
    BEARER_TOKEN : str
        A Twitter API bearer token.
    PARAMS : dict
        A dictionary parsed to the header of the API URL request
    verbose : bool
        A boolean indicating whether progress of the download
        routine should be printed to the console. Defaults to
        True.
    save_file : bool
        A boolean indicating whether the resulting data frame
        with all tweets should be saved to a csv file including
        a timestamp in the file name (since download output
        may depend on the time of download). Defaults to True.
  
    Returns
    -------
    pandas.DataFrame
        A pandas data frame including all tweets with one row
        representing one tweet and the variables specified in
        the PARAMS argument parsed through the
        pandas.json_normalize() function.
        
    Raises
    ------
    ApiError
        Either the request status code was not 200
        or the PARAMS argument is malformed (but not
        invalid) to ensure that the pagination routine
        works as intended.
    """
    if 'pagination_token' in PARAMS.keys():
        del PARAMS['pagination_token']
        
    if 'max_results' not in PARAMS.keys() or int(PARAMS['max_results']) != 100:
        raise ApiError('Please ensure that you parse max_results: 100 to '\
                       'your requests parameters.')
    
    # Prepare URL request
    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {BEARER_TOKEN}'})
    URL = f"https://api.twitter.com/2/users/{ACCOUNT_ID}/tweets"
    request_count = 0
    
    while (request_count < 32):
        req = requests.models.PreparedRequest()
        req.prepare_url(URL, PARAMS)
        req = s.get(req.url)
        # Sleep for 1 second due to rate limit
        time.sleep(1)
        
        if req.status_code != 200:
            raise ApiError(f'There was an error sending request '\
                           f'{request_count+1}. Status code '\
                           f'{req.status_code} message {req.content}')

        # Get content, paginate and save rows to df
        page = json.loads(req.content)
        if 'data' not in page or 'meta' not in page: break
        if page['meta']['result_count'] == 0 and request_count == 0:
            if verbose:
                print(f'No results found for {ACCOUNT_ID}! Returning '\
                      f'empty data frame.')
            if save_file:
                print('Note: Results will not be written to a '\
                      'timestamped file.')
            return pd.DataFrame()
        
        # If it is the first request, initialize data frame
        if request_count == 0: 
            df = pd.json_normalize(page['data'])
        # Else append results to existing data frame
        else:
            df = df.append(pd.json_normalize(page['data']))
        
        if 'next_token' not in page['meta'].keys():
            if verbose:
                print(f'All tweets for {ACCOUNT_ID} found after '\
                      f'{request_count+1} API calls.')
            break
        
        # Update pagination token and request_count
        NEXT_TOKEN = page['meta']['next_token']
        PARAMS['pagination_token'] = NEXT_TOKEN
        request_count += 1
        
        if verbose: 
            print(f'Request {request_count} successful. Sleeping 1 '\
                  f'second and paginating.')
        
    if verbose:
        print(f'All most recent for account {ACCOUNT_ID} downloaded.')
        
    if save_file:
        # Save file with timestamp
        fn = f"json/{ACCOUNT_ID}_"\
             f"{datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.csv"
        print(f'Saving file to {fn}')
        try: df.to_csv(fn, index=False)
        except: return
        
    return df

def download_and_save_nhsuk_tweets(token_file_path='bearer_token.txt', 
                                   user_name='nhsuk', verbose=True, 
                                   save_file=True): 
    """
    This is a wrapper function for the
    get_most_recent_tweets_account() function to download 
    the most recent tweets of a specified Twitter account
    through the Twitter API. The request parameters
    (i.e. variable requested) are hard-coded in this
    function for this study. For more information 
    go to the documentation of the get_most_recent_tweets_account() 
    function via help(get_most_recent_tweets_account).
    
    Parameters
    ----------
    token_file_path : str
        Readable file path containing a document which includes
        the Twitter API bearer token in its first line. Defaults
        to 'bearer_token.txt'.
    user_name : str
        The username to look up (either including or 
        excluding) the '@' symbol.
    verbose : bool
        A boolean indicating whether progress of the download
        routine should be printed to the console. Defaults to
        True.
    save_file : bool
        A boolean indicating whether the resulting data frame
        with all tweets should be saved to a csv file including
        a timestamp in the file name (since download output
        may depend on the time of download). Defaults to True.
  
    Returns
    -------
    pandas.DataFrame
        A pandas data frame including all tweets with one row
        representing one tweet and the variables specified in
        the PARAMS argument parsed through the
        pandas.json_normalize() function.
    
    """
    BEARER_TOKEN = read_bearer_token(token_file_path)
    TWITTER_USER_ID = look_up_twitter_acount_id(BEARER_TOKEN, user_name)

    PARAMS = {
        "max_results": "100", # maximum number of results permitted
        "tweet.fields": "author_id,created_at,text,conversation_id", 
        "expansions":  "referenced_tweets.id"
    }

    df = get_most_recent_tweets_account(TWITTER_USER_ID, BEARER_TOKEN, 
                                        PARAMS, verbose=verbose, 
                                        save_file=save_file)
    return df

def get_conversation(CONV_ID, BEARER_TOKEN, PARAMS, verbose=True):
    """
    A subroutine to download all tweets attached to a specific
    conversation ID, including the possibility for pagination.
    Since this will be used in a pipeline of downloading
    multiple conversations, it only produces warnings instead
    of raising an error.
    
    The function uses the 
    GET /2/tweets/search/all endpoint of the Twitter V2 API. The 
    endpoint allows for 300 requests per 15-minute window. Hence, 
    the thread pauses for 3 second in between API calls. More 
    information on the endpoint can be found here:
    https://developer.twitter.com/en/docs/twitter-api/tweets/search/api-reference/get-tweets-search-all
    
    Parameters
    ----------
    CONV_ID : str
        The conversation ID that ought to be downloaded.
    BEARER_TOKEN : str
        A Twitter API bearer token.
    PARAMS : dict
        A dictionary parsed to the header of the API URL request
    verbose : bool
        A boolean indicating whether progress of the download
        routine should be printed to the console. Defaults to
        True. Does not affect warnings raised after malformed
        API returns.
  
    Returns
    -------
    pandas.DataFrame
        A pandas data frame including all tweets with one row
        representing one tweet and the variables specified in
        the PARAMS argument parsed through the
        pandas.json_normalize() function.
        
    Raises
    ------
    ApiError
        If PARAMS argument is malformed (but not
        invalid) to ensure that the pagination routine
        works as intended.
    """    
    # Delete next_token from previous subroutine
    if 'next_token' in PARAMS.keys(): 
        del PARAMS['next_token']
        
    if 'max_results' not in PARAMS.keys() or int(PARAMS['max_results']) != 500:
        raise ApiError('Please ensure that you parse max_results: 500 into '\
                       'your requests parameters.')
    
    s = requests.Session()
    s.headers.update({'Authorization': f'Bearer {BEARER_TOKEN}'})
    URL = f"https://api.twitter.com/2/tweets/search/all"\
          f"?query=conversation_id:{CONV_ID}"
    
    returned_less_than_500_tweets = False
    request_count = 0
    
    while not returned_less_than_500_tweets:    
        req = requests.models.PreparedRequest()
        req.prepare_url(URL, PARAMS)
        req = s.get(req.url)
        time.sleep(3) # rate limit
    
        if req.status_code != 200:
            warnings.warn(f'CONV ID had a warning, try to re-download '\
                          f'{CONV_ID} and inspect the page.')
            # Return empty frame to not break pipeline
            print('Last page:')
            print(json.loads(req.content))
            print('Returning empty data frame.')
            return pd.DataFrame()

        page = json.loads(req.content)
        
        if page['meta']['result_count'] == 0 and request_count == 0:
            if verbose:
                print(f'No results found for {CONV_ID}! Returning empty '\
                      f'data frame.')
            return pd.DataFrame()
        
        # If it is the first request, initialize data frame
        if request_count == 0: # first request, initialize df
            df = pd.json_normalize(page['data'])
        # Else append results to existing data frame
        else: 
            df = df.append(pd.json_normalize(page['data']))
        
        if 'next_token' not in page['meta'].keys():
            returned_less_than_500_tweets = True
        else:
            NEXT_TOKEN = page['meta']['next_token']
            PARAMS['next_token'] = NEXT_TOKEN
            if verbose:
                print(f'Querying next token {NEXT_TOKEN} for '\
                      f'conversation {CONV_ID}...')
        
        request_count += 1
    
    return df
    
def get_conversations(CONV_ID_ARRAY, BEARER_TOKEN, PARAMS, verbose=True, 
                      save_file=True, reference='NHSUK'):
    """
    A routine to download, combine, and save all 
    conversations in an array of conversation IDs through the
    get_conversation() function. For more information 
    go to the documentation of the get_conversation() function
    via help(get_conversation).
    
    Parameters
    ----------
    CONV_ID_ARRAY : iterable
        An iterable (e.g., list, numpu array) holding all 
        conversation IDs that ought to be downloaded.
    BEARER_TOKEN : str
        A Twitter API bearer token.
    PARAMS : dict
        A dictionary parsed to the header of the API URL request
    verbose : bool
        A boolean indicating whether progress of the download
        routine should be printed to the console. Defaults to
        True. Does not affect warnings raised after malformed
        API returns.
    save_file : bool
        A boolean indicating whether the resulting data frame
        with all tweets should be saved to a csv file including
        a timestamp in the file name (since download output
        may depend on the time of download). Defaults to True.
    reference : str
        A string reference to be included in the csv file to
        which the results are written. Defaults to 'NHSUK'.
  
    Returns
    -------
    pandas.DataFrame
        A pandas data frame including all tweets with one row
        representing one tweet and the variables specified in
        the PARAMS argument parsed through the
        pandas.json_normalize() function.
        
    Raises
    ------
    ApiError
        If PARAMS argument is malformed (but not
        invalid) to ensure that the pagination routine
        works as intended.
    """
    # Initialize list of data frames holding data frames
    # representing indiviudal conversations and 
    # progress tracking variables.
    dfs = []
    count = 1
    n_convs = len(CONV_ID_ARRAY)
    for CONV_ID in CONV_ID_ARRAY:
        if verbose:
            print(f'Downloading conversation {CONV_ID}.')
        # Run subroutine for conversation ID
        df = get_conversation(CONV_ID, BEARER_TOKEN, PARAMS)
        dfs.append(df)
        if verbose:
            percent_done = round(count*100/n_convs,2)
            print(f'Downloaded conversation {CONV_ID} and downloaded '\
                  f'{percent_done}%!')
        count += 1
    # Combine results into a single data frame
    res = pd.concat(dfs)
    if save_file:
        fn = f"Conversations_{reference}_"\
             f"{datetime.datetime.now().strftime('%Y-%m-%d_%H:%M:%S')}.csv"
        print(f'Saving file to {fn}')
        res.to_csv(fn, index=False)
    return res

def extract_and_download_conversation_ids(df, 
                                          token_file_path='bearer_token.txt',
                                          verbose=True, save_file=True, 
                                          reference='NHSUK'):
    """
    A wrapper function to extract all unique 
    conversation IDs from a data frame with tweets
    (in the case of this study, the 3,200 most recent NHSUK tweets)
    and then call the get_conversations() routine to
    download all of these conversations.
    
    For more information 
    go to the documentation of these functions
    via help(get_conversation) or
    help(get_conversations).
    
    Parameters
    ----------
    df : pandas.DataFrame
        A data frame of tweets whose conversation IDs ought
        to be downloaded.
    token_file_path : str
        Readable file path containing a document which includes
        the Twitter API bearer token in its first line. Defaults
        to 'bearer_token.txt'.
    verbose : bool
        A boolean indicating whether progress of the download
        routine should be printed to the console. Defaults to
        True. Does not affect warnings raised after malformed
        API returns.
    save_file : bool
        A boolean indicating whether the resulting data frame
        with all tweets should be saved to a csv file including
        a timestamp in the file name (since download output
        may depend on the time of download). Defaults to True.
    reference : str
        A string reference to be included in the csv file to
        which the results are written. Defaults to 'NHSUK'.
  
    Returns
    -------
    pandas.DataFrame
        A pandas data frame including all tweets for all tweets
        attached to the conversation IDs of the parsed data frame
        with one row representing one tweet and the variables 
        specified in the PARAMS argument parsed through the
        pandas.json_normalize() function.
        
    Raises
    ------
    ApiError
        If no 'conversation_id' columns to extract conversation
        IDs from is found in the parsed data frame.
    """
    # Will raise error if df is not type pandas.DataFrame
    if 'conversation_id' not in df.columns:
        raise ApiError('Could not find column holding conversation IDs.')
    
    # Extract all unique conversation IDs for downloading
    conv_ids = pd.unique(df['conversation_id'])
    n_convs = len(conv_ids)
    print(f'Found {n_convs} unique conversation IDs!')
    
    # Specify API parameters
    PARAMS = {
        "max_results": "500", # maximum number of results permitted
        "tweet.fields": "author_id,created_at,text,conversation_id",
        "expansions":  "referenced_tweets.id"
    }
    
    # Get token
    BEARER_TOKEN = read_bearer_token(token_file_path)

    # Download conversations
    df_conv = get_conversations(conv_ids, BEARER_TOKEN, PARAMS, 
                                verbose=verbose, save_file=save_file, 
                                reference=reference)
    return df_conv

BEARER_TOKEN = read_bearer_token()
PARAMS = {
'max_results': "100",
'start_time': "2010-11-06T00:00:00Z", # (YYYY-MM-DDTHH:mm:ssZ) -> RFC3339 date-time
'end_time': "2023-01-31T23:59:59Z", # (YYYY-MM-DDTHH:mm:ssZ)
'tweet.fields': "attachments,author_id,conversation_id,created_at,entities,geo,id,in_reply_to_user_id,lang,possibly_sensitive,public_metrics,referenced_tweets,reply_settings,source,text,withheld",
'expansions': "attachments.poll_ids,attachments.media_keys,author_id,geo.place_id,in_reply_to_user_id,referenced_tweets.id,entities.mentions.username,referenced_tweets.id.author_id",
'media.fields': "duration_ms,height,media_key,preview_image_url,type,url,width,public_metrics,alt_text",
'place.fields': "contained_within,country,country_code,full_name,geo,id,name,place_type",
'poll.fields': "duration_minutes,end_datetime,id,options,voting_status",
'user.fields': "created_at,description,entities,id,location,name,pinned_tweet_id,profile_image_url,protected,public_metrics,url,username,verified,withheld",
}
