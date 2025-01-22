#!/usr/local/bin/python3.12

from decimal import Decimal
import json
import boto3
import os
from botocore.exceptions import ClientError
from datetime import datetime


# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")

# DynamoDB table references
raw_comments_table_name = os.environ.get("RAW_COMMENTS_TABLE_NAME", "RawCommentsTable")
sentiment_scores_table_name = os.environ.get("SENTIMENT_SCORES_TABLE_NAME", "SentimentScoresTable")
raw_comments_table = dynamodb.Table(raw_comments_table_name)
sentiment_scores_table = dynamodb.Table(sentiment_scores_table_name)


def fetch_sentiment_scores(video_id, last_updated_at):
    """
    Fetch comments for a specific video_id processed after the last_updated_at timestamp.
    """
    try:
        # Fetch comments with processed_at > last_updated_at
        response = raw_comments_table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key("video_id").eq(video_id),
            FilterExpression=boto3.dynamodb.conditions.Attr("processed_at").gt(last_updated_at),
        )
        return response.get("Items", [])
    except ClientError as e:
        print(f"Error fetching comments for video_id {video_id}: {e}")
        return []


def fetch_video_metadata(video_id):
    """
    Fetch video metadata from the SentimentScoresTable.
    """
    try:
        response = sentiment_scores_table.get_item(Key={"video_id": video_id})
        return response.get("Item")
    except ClientError as e:
        print(f"Error fetching metadata for video_id {video_id}: {e}")
        return None


def update_video_metadata(video_id, overall_score, comment_count, latest_timestamp):
    """
    Update or insert video metadata in the SentimentScoresTable.
    """
    try:
        sentiment_scores_table.put_item(
            Item={
                "video_id": video_id,
                "overall_score": overall_score,
                "comment_count": comment_count,
                "last_updated_at": latest_timestamp,
            }
        )
        print(f"Updated metadata for video_id {video_id}.")
    except ClientError as e:
        print(f"Error updating metadata for video_id {video_id}: {e}")


def calculate_overall_score(existing_score, existing_count, new_comments):
    """
    Calculate the new overall score by combining the existing score and new comments.
    """
    new_score = 0
    for comment in new_comments:
        sentiment_score = comment.get("sentiment_score", {})
        positive_score = Decimal(sentiment_score.get("Positive", "0"))
        negative_score = Decimal(sentiment_score.get("Negative", "0"))
        new_score += positive_score - negative_score

    new_count = len(new_comments)
    total_count = existing_count + new_count

    if total_count == 0:
        return 0

    # Weighted average score
    updated_score = (
        (existing_score * existing_count) + (new_score / new_count * new_count)
    ) / total_count

    return round((updated_score + 1) * 50), total_count


def lambda_handler(event, context):
    """
    Entry point for the Lambda function.
    """
    print(f"Received event: {json.dumps(event, indent=2)}")  # Debugging

    # Validate input
    if "video_id" not in event:
        print("Error: Missing video_id in the event.")
        return {
            "statusCode": 400,
            "body": json.dumps({"error": "Missing video_id in the event."}),
        }

    video_id = event["video_id"]
    print(f"Processing video ID: {video_id}")

    # Fetch metadata for the video
    metadata = fetch_video_metadata(video_id)

    if metadata:
        existing_score = Decimal(metadata["overall_score"])
        existing_count = metadata["comment_count"]
        last_updated_at = metadata["last_updated_at"]
    else:
        # Initialize if no metadata exists
        existing_score = Decimal(0)
        existing_count = 0
        last_updated_at = "1970-01-01T00:00:00.000000"

    # Fetch new comments processed after the last updated timestamp
    new_comments = fetch_sentiment_scores(video_id, last_updated_at)

    if not new_comments:
        print(f"No new comments to process for video_id {video_id}.")
        return {
            "statusCode": 200,
            "body": json.dumps({"message": "No new comments to process."}),
        }

    # Calculate updated overall score
    overall_score, updated_count = calculate_overall_score(existing_score, existing_count, new_comments)

    # Get the latest processed_at timestamp from new comments
    latest_timestamp = max(comment["processed_at"] for comment in new_comments)

    # Update SentimentScoresTable
    update_video_metadata(video_id, overall_score, updated_count, latest_timestamp)

    return {
        "statusCode": 200,
        "body": json.dumps(
            {
                "message": "Score updated successfully.",
                "video_id": video_id,
                "overall_score": overall_score,
                "comment_count": updated_count,
            }
        ),
    }