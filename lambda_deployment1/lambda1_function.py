#!/usr/local/bin/python3.12

import json
import re
import os
import requests
import boto3

# Fetch the YouTube API key from environment variables
API_KEY = os.getenv("YOUTUBE_API_KEY")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")

# Initialize SQS client
sqs = boto3.client('sqs')

def get_video_id(url):
    """
    Extracts the YouTube video ID from a URL.
    """
    video_id_match = re.search(r"v=([a-zA-Z0-9_-]{11})", url)
    if video_id_match:
        return video_id_match.group(1)
    else:
        raise ValueError("Invalid YouTube URL. Please provide a valid video URL.")

def fetch_youtube_comments(api_key, video_id, page_token=None, order="relevance"):
    """
    Fetches up to 50 comments for a YouTube video using the YouTube Data API.
    Supports pagination using `page_token` and sorting by `order` (e.g., relevance or time).
    """
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "key": api_key,
        "maxResults": 50,
        "order": order,
    }
    if page_token:
        params["pageToken"] = page_token

    response = requests.get(url, params=params)
    if response.status_code == 200:
        data = response.json()
        comments = [
            {
                "text": item["snippet"]["topLevelComment"]["snippet"]["textOriginal"],
                "author": item["snippet"]["topLevelComment"]["snippet"]["authorDisplayName"],
                "published_at": item["snippet"]["topLevelComment"]["snippet"]["publishedAt"]
            }
            for item in data.get("items", [])
        ]
        return comments, data.get("nextPageToken")  # Return nextPageToken for pagination
    else:
        raise Exception(f"Error: {response.status_code}, {response.text}")

def send_to_sqs(queue_url, message):
    """
    Sends a message to the specified SQS queue.
    """
    response = sqs.send_message(
        QueueUrl=queue_url,
        MessageBody=json.dumps(message)
    )
    return response

def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    Accepts an event with a 'video_url' key in the JSON body.
    If 'page_token' is provided, uses it to fetch the next set of comments.
    """
    try:
        # Parse the incoming request body as JSON
        body = json.loads(event["body"])
        video_url = body.get("video_url")
        if not video_url:
            return {
                "statusCode": 400,
                "body": json.dumps({"error": "No video URL provided."})
            }

        # Extract additional parameters
        page_token = body.get("page_token")  # For fetching the next page of comments
        order = "relevance"  # Always use relevance for this use case

        # Extract the video ID
        video_id = get_video_id(video_url)

        # Fetch comments
        comments, next_page_token = fetch_youtube_comments(API_KEY, video_id, page_token, order)

        # Create a message for SQS
        message = {
            "video_url": video_url,
            "video_id": video_id,
            "comments": comments,
            "next_page_token": next_page_token,  # Include next page token for subsequent fetches
        }

        # Send the message to SQS
        send_to_sqs(SQS_QUEUE_URL, message)

        # Return success response
        return {
            "statusCode": 200,
            "body": json.dumps({
                "message": "Comments successfully sent to SQS.",
                "next_page_token": next_page_token,  # Return next page token
            })
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }