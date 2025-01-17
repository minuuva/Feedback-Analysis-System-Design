#!/usr/local/bin/python3.12

import json
import re
import os
import requests
import boto3

# Fetch the YouTube API key from environment variables
API_KEY = os.getenv("YOUTUBE_API_KEY")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")  # Add your SQS queue URL here

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

def fetch_top_youtube_comments(api_key, video_id):
    """
    Fetches the top 10 comments for a YouTube video using the YouTube Data API.
    """
    url = "https://www.googleapis.com/youtube/v3/commentThreads"
    params = {
        "part": "snippet",
        "videoId": video_id,
        "key": api_key,
        "maxResults": 10,
        "order": "relevance"
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

        # Extract the video ID
        video_id = get_video_id(video_url)

        # Fetch the top comments
        comments = fetch_top_youtube_comments(API_KEY, video_id)

        # Create a message for SQS
        message = {
            "video_url": video_url,
            "video_id": video_id,
            "comments": comments
        }

        # Send the message to SQS
        send_to_sqs(SQS_QUEUE_URL, message)

        # Return success response
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Comments successfully sent to SQS."})
        }

    except Exception as e:
        return {
            "statusCode": 500,
            "body": json.dumps({"error": str(e)})
        }