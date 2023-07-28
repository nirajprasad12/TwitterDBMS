#!/usr/bin/env python
# coding: utf-8

# In[127]:


import configparser
import tweepy
import pandas as pd
import pip
import re
import io
import csv
import numpy as np
import json
from azure.storage.blob import BlobClient

# Authorization

api_key = "haRYjJxU7W4QgFna7ixmOG6AH"
api_key_secret = "eDdTSUwD38auxiWz8acsIZI0pb53H7fJKDn1dtctBg2C3eweMO"

access_token = "1582810070458331136-nmw6T5zanyDMEVtcXs7XmwF4fASz0B"
access_token_secret = "26hPMFiOClj1TBSgUvyg7OPkLFGF8rpgkmjj0hy3xfiES"


auth = tweepy.OAuthHandler(api_key, api_key_secret)
auth.set_access_token(access_token, access_token_secret)
api = tweepy.API(auth)


connectionString = "DefaultEndpointsProtocol=https;AccountName=storageaccountsindhu;AccountKey=rAutmbrwSu9NpeQfG67RAIyxZNuEgKx+NxF2kYYqWk+Nrt42+DPnwMVj2UX/5e9Hs0I8cryRC/Ry+ASt0xpEbA==;EndpointSuffix=core.windows.net"
containerName = "tweetblob/output"


# In[131]:


query = 'America'


# In[132]:


# Twitter Header data arrays:
tweet_id = np.empty((0,), dtype=str)
twitter_handle = np.empty((0,), dtype=str)
parent_tweet_id = np.empty((0,), dtype=str)
tweet = []
tweet_time = []
location = []

# Twitter User data arrays:
user_name = []
profile_img_url = []
description = []
follower_count = []
following_count = []
joined_on = []

#hashtags
hashtags = []
hashtags_text = []


# In[133]:



tweets = tweepy.Cursor(api.search_tweets, q=query, lang='en').items(88)
for status in tweets:
    
    user_name = np.append(user_name, status.user.name)
    profile_img_url = np.append(profile_img_url, status.user.profile_image_url)
    description = np.append(description, status.user.description)
    follower_count = np.append(follower_count, status.user.followers_count)
    following_count = np.append(following_count, status.user.friends_count)
    joined_on = np.append(joined_on, status.user.created_at)
    
    tweet_id = np.append(tweet_id, status.id)
    twitter_handle = np.append(twitter_handle, status.user.id)
    parent_tweet_id = np.append(parent_tweet_id, status.in_reply_to_status_id)
    tweet = np.append(tweet, status.text)
    tweet_time = np.append(tweet_time, status.created_at)
    location = np.append(location, status.user.location)
    
    hashtags = np.append(hashtags, status.entities["hashtags"])


# In[134]:


twitter_header = pd.DataFrame({'tweet_id': tweet_id, 'twitter_handle': twitter_handle, 'parent_tweet_id': parent_tweet_id, 'tweet': tweet, 'tweet_time': tweet_time, 'location': location})

twitter_header = twitter_header.astype({"parent_tweet_id": str})

twitter_header = twitter_header[pd.to_numeric(twitter_header['tweet_id'], errors='coerce').notna()]
# In[135]:


twitter_user = pd.DataFrame({
'twitter_handle': twitter_handle,
'user_name': user_name,
'profile_img_url': profile_img_url,
'description': description,
'follower_count': follower_count,
'following_count': following_count,
'joined_on': joined_on,
'location': location
})


# In[136]:


twitter_user = twitter_user.drop_duplicates()
twitter_header = twitter_header.drop_duplicates()


twitter_header = twitter_header.replace('~', ' ', regex=True)


# Twitter Header

outputBlobName	= "twitterheader.csv"
blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)

twitter_header.to_csv(outputBlobName,sep = "~",encoding = "utf-8", index = False)

with open(outputBlobName, "rb") as data:
    blob.upload_blob(data, overwrite=True)




# Twitter User
# twitter_user.to_csv('data/twitteruser.csv',sep = ",",encoding = "utf-8", index = False)

twitter_user = twitter_user.replace('~', ' ', regex=True)

outputBlobName	= "twitter_user.csv"
blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)

twitter_user.to_csv(outputBlobName,sep = "~",encoding = "utf-8", index = False)

with open(outputBlobName, "rb") as data:
    blob.upload_blob(data, overwrite=True)


# Twitter URL

tweet_url = pd.DataFrame({'tweet_id': tweet_id})
tweet_url['URL'] = 'https://twitter.com/twitter/statuses/' + tweet_url['tweet_id']

tweet_url = tweet_url.drop_duplicates()

outputBlobName	= "tweet_url.csv"
blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)

tweet_url.to_csv(outputBlobName,sep = "~",encoding = "utf-8", index = False)

with open(outputBlobName, "rb") as data:
    blob.upload_blob(data, overwrite=True)



# referenced_tweet_table

referenced_tweet_table = pd.DataFrame()
referenced_tweet_table['child_tweet_id'] = twitter_header[twitter_header['parent_tweet_id'] != 'None']['tweet_id']
referenced_tweet_table['parent_tweet_id'] = twitter_header[twitter_header['parent_tweet_id'] != 'None']['parent_tweet_id']
referenced_tweet_table['parent_tweet_user_id'] = ''
referenced_tweet_table['parent_tweet_user_name'] = ''
referenced_tweet_table['parent_tweet_text'] = ''

referenced_tweet_table.reset_index(drop = True, inplace = True)

for i in range(0, len(referenced_tweet_table['parent_tweet_id'])):
    tweet = api.get_status(referenced_tweet_table['parent_tweet_id'][i])
    referenced_tweet_table['parent_tweet_user_id'][i] = tweet.user.id_str
    referenced_tweet_table['parent_tweet_user_name'][i] = tweet.user.name
    referenced_tweet_table['parent_tweet_text'][i] = tweet.text

referenced_tweet_table = referenced_tweet_table.drop_duplicates()

referenced_tweet_table = referenced_tweet_table.replace('~', ' ', regex=True)

outputBlobName	= "referenced_tweet_table.csv"
blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)

referenced_tweet_table.to_csv(outputBlobName,sep = "~",encoding = "utf-8", index = False)

with open(outputBlobName, "rb") as data:
    blob.upload_blob(data, overwrite=True)


# Hashtags data

hashtagsdata = {}

for hashtag in hashtags:
    hashtagsdata[hashtag['text']] = []
    tweets = tweepy.Cursor(api.search_tweets, q = hashtag['text'], lang='en').items(10)
    for tweet in tweets:
        hashtagsdata[hashtag['text']].append({'username': tweet.user.name, 'tweet_location': tweet.user.location, 'tweet_text': tweet.text})

df = [d['text'] for d in hashtags]

js_hashtags = []

for i in df:
    obj = {
        "hashtag": i,
        "tweets": []
    }
    
    tweets = tweepy.Cursor(api.search_tweets, q = i, lang='en').items(10)
    for tweet in tweets:
        child_obj = {'username': tweet.user.name, 'tweet_location': tweet.user.location, 'tweet_text': tweet.text}
        obj["tweets"].append(child_obj)
    js_hashtags.append(obj)

outputBlobName	= "hashtagsdata.json"
blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)

with open(outputBlobName, "w") as outfile:
    json.dump(js_hashtags, outfile)

with open(outputBlobName, "rb") as data:
    blob.upload_blob(data, overwrite=True)


# Graph data model

max_len = max(len(v) for v in hashtagsdata.values())


for k, v in hashtagsdata.items():
   if len(v) < max_len:
       hashtagsdata[k] += [None] * (max_len - len(v))


rows = list(zip(*hashtagsdata.values()))

df = pd.DataFrame(rows, columns=hashtagsdata.keys())

df = df.reset_index()

melted_df = pd.melt(df, id_vars=['index'], var_name='variable', value_name='value')

melted_df = melted_df.dropna(how = 'any')

melted_df.reset_index(drop = True, inplace = True)

melted_df['username'] = np.nan * np.empty(len(melted_df))
melted_df['location'] = np.nan * np.empty(len(melted_df))

for i in range(0, len(melted_df['value'])):
    melted_df['username'][i] = melted_df['value'][i]['username']
    melted_df['location'][i] = melted_df['value'][i]['tweet_location']


nodes = pd.merge(melted_df[melted_df['location'] != ''],
        twitter_header[twitter_header['location'] != ''], on='location', how = 'inner')


edge_df = nodes[['variable', 'location']]
edge_df.columns = ['_from', '_to']

edge_df['type'] = 'used_in'
edge_df['_from'] = 'nodes/' + edge_df['_from']
edge_df['_to'] = 'nodes/' + edge_df['_to']

nodes = nodes.replace('~', ' ', regex=True)

outputBlobName	= "nodes.csv"
blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)

nodes.to_csv(outputBlobName,sep = "~",encoding = "utf-8", index = False)

with open(outputBlobName, "rb") as data:
    blob.upload_blob(data, overwrite=True)

edge_df = edge_df.replace('~', ' ', regex=True)

outputBlobName	= "edge_df.csv"
blob = BlobClient.from_connection_string(conn_str=connectionString, container_name=containerName, blob_name=outputBlobName)

edge_df.to_csv(outputBlobName,sep = "~",encoding = "utf-8", index = False)

with open(outputBlobName, "rb") as data:
    blob.upload_blob(data, overwrite=True)