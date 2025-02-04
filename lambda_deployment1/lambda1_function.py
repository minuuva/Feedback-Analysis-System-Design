#!/usr/local/bin/python3.12

import json
import re
import os
import requests
import boto3
from botocore.exceptions import ClientError

# Fetch environment variables
API_KEY = os.getenv("YOUTUBE_API_KEY")
SQS_QUEUE_URL = os.getenv("SQS_QUEUE_URL")
# Update the default to "PaginationState" to match the table you created.
PAGINATION_STATE_TABLE = os.getenv("PAGINATION_STATE_TABLE", "PaginationState")

# Initialize AWS clients
sqs = boto3.client('sqs')
dynamodb = boto3.resource('dynamodb')

def ensure_state_table_exists(table_name):
    """
    Ensures that the DynamoDB table for storing pagination state exists.
    If it does not, creates the table with a simple key schema.
    (If you already created the table manually, this will simply load it.)
    """
    try:
        table = dynamodb.Table(table_name)
        # Attempt to load table metadata; this will fail if the table does not exist.
        table.load()
        return table
    except ClientError as e:
        error_code = e.response['Error']['Code']
        if error_code == 'ResourceNotFoundException':
            print(f"Table {table_name} not found. Creating new table...")
            table = dynamodb.create_table(
                TableName=table_name,
                KeySchema=[
                    {'AttributeName': 'video_id', 'KeyType': 'HASH'}
                ],
                AttributeDefinitions=[
                    {'AttributeName': 'video_id', 'AttributeType': 'S'}
                ],
                ProvisionedThroughput={
                    'ReadCapacityUnits': 1,
                    'WriteCapacityUnits': 1
                }
            )
            table.wait_until_exists()
            print(f"Table {table_name} created successfully.")
            return table
        else:
            raise

# Ensure the pagination state table exists (this will now load your pre-created "PaginationState" table)
state_table = ensure_state_table_exists(PAGINATION_STATE_TABLE)

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
    Supports pagination using page_token and sorting by order (e.g., relevance or time).
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

def get_next_page_token_from_state(video_id):
    """
    Retrieve the stored nextPageToken for a given video_id from the pagination state table.
    """
    try:
        response = state_table.get_item(Key={"video_id": video_id})
        if "Item" in response:
            return response["Item"].get("next_page_token")
        else:
            return None
    except ClientError as e:
        print(f"Error retrieving pagination state for video {video_id}: {str(e)}")
        return None

def update_next_page_token_in_state(video_id, next_page_token):
    """
    Updates the stored nextPageToken for a given video_id in the pagination state table.
    """
    try:
        state_table.put_item(Item={"video_id": video_id, "next_page_token": next_page_token})
    except ClientError as e:
        print(f"Error updating pagination state for video {video_id}: {str(e)}")

def lambda_handler(event, context):
    """
    AWS Lambda entry point.
    Accepts an event with a 'video_url' key in the JSON body.
    If 'page_token' is provided, uses it to fetch the next set of comments.
    Otherwise, it checks the persistent state store for a saved token.
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

        # Extract additional parameters from the event, if provided
        provided_page_token = body.get("page_token")  # For fetching the next page of comments

        # Extract the video ID
        video_id = get_video_id(video_url)

        # If no page token is provided in the event, check the state store
        if not provided_page_token:
            page_token = get_next_page_token_from_state(video_id)
        else:
            page_token = provided_page_token

        order = "relevance"  # Always use relevance for this use case

        # Fetch comments using the determined page token (could be None)
        comments, next_page_token = fetch_youtube_comments(API_KEY, video_id, page_token, order)

        # Update pagination state in DynamoDB so the next run will pick up the next page
        update_next_page_token_in_state(video_id, next_page_token)

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