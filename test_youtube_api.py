#!/usr/local/bin/python3.12
import os
import re
import requests

# Fetch the YouTube API key from environment variables
API_KEY = os.getenv("YOUTUBE_API_KEY")

def get_video_id(url):
    """
    Extracts the YouTube video ID from a URL.
    """
    video_id_match = re.search(r"v=([a-zA-Z0-9_-]{11})", url)
    if video_id_match:
        return video_id_match.group(1)
    else:
        raise ValueError("Invalid YouTube URL. Please provide a valid video URL.")

def fetch_top_youtube_comments(api_key, video_id):
    """
    Fetches the top 10 comments for a YouTube video using the YouTube Data API.
    """
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",              # Required parameter
        "videoId": video_id,            # Pass the video ID here
        "key": api_key,                 # API key
        "maxResults": 10,               # Number of comments to fetch
        "order": "relevance"            # Fetch top comments
    }
    response = requests.get(url, params=params)
    if response.status_code == 200:
        comments = [
            {
                "text": item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                "author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                "published_at": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            }
            for item in response.json().get("items", [])
        ]
        return comments
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

def main():
    """
    Main function to interact with the user and display comments.
    """
    # Step 1: Ask the user for a YouTube video URL
    video_url = input("Enter the YouTube video URL: ")
    
    try:
        # Step 2: Extract the video ID
        video_id = get_video_id(video_url)
        print(f"Extracted Video ID: {video_id}")
        
        # Step 3: Fetch the top comments
        comments = fetch_top_youtube_comments(API_KEY, video_id)
        print("\nTop Comments:")
        for idx, comment in enumerate(comments, start=1):
            print(f"{idx}. {comment['text']}")
            print(f"   - By: {comment['author']}")
            print(f"   - Published At: {comment['published_at']}")
            print()
    
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()