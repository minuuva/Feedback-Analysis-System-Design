#!/usr/local/bin/python3.12
import os
import requests

# Replace these with your API key and a YouTube video ID
API_KEY = os.getenv("YOUTUBE_API_KEY")
VIDEO_ID = "DSY7u8Jg9c0"  # Replace with a valid YouTube video ID

def fetch_top_youtube_comments(api_key, video_id):
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",              # Fetch comment details
        "videoId": video_id,            # The video ID
        "key": api_key,                 # Your API key
        "maxResults": 10,               # Limit to 10 comments
        "order": "relevance"            # Fetch top comments based on relevance
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        comments = [
            item["snippet"]["topLevelComment"]["snippet"]["textOriginal"]
            for item in response.json().get("items", [])
        ]
        return comments
    else:
        return f"Error: {response.status_code}, {response.text}"

# Fetch and print the top 10 comments
top_comments = fetch_top_youtube_comments(API_KEY, VIDEO_ID)
print("Top 10 Comments:")
for idx, comment in enumerate(top_comments, start=1):
    print(f"{idx}: {comment}")
