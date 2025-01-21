#!/usr/local/bin/python3.12

from decimal import Decimal
import json
import boto3
import os
from botocore.exceptions import ClientError
from time import sleep

# Initialize DynamoDB and Comprehend clients
dynamodb = boto3.resource("dynamodb")
comprehend = boto3.client("comprehend")

# Reference the RAW Data from the DynamoDB table
table_name = os.environ.get("DYNAMODB_TABLE_NAME", "RawCommentsTable")
table = dynamodb.Table(table_name)

def analyze_sentiment(text):
    """
    Use AWS Comprehend to analyze the sentiment of a comment.
    """
    try:
        response = comprehend.detect_sentiment(Text=text, LanguageCode="en")
        sentiment_score = {
            key: Decimal(str(value))
            for key, value in response["SentimentScore"].items()
        }
        return response["Sentiment"], sentiment_score
    except comprehend.exceptions.TextSizeLimitExceededException as e:
        print(f"Text too long for sentiment analysis: {e}")
    except comprehend.exceptions.ThrottlingException as e:
        print("Throttling error, retrying...")
        sleep(1)
        return analyze_sentiment(text)
    except ClientError as e:
        print(f"Error analyzing sentiment: {e}")
    return None, None

def update_comments_batch(processed_comments):
    """
    Batch update comments with sentiment analysis results.
    """
    try:
        with table.batch_writer() as batch:
            for comment in processed_comments:
                batch.put_item(Item=comment)
        print(f"Batch updated {len(processed_comments)} comments.")
    except ClientError as e:
        print(f"Error updating comments batch: {e}")

def aggregate_sentiment_scores(comments):
    """
    Aggregates sentiment scores to calculate an overall sentiment score for the video.
    """
    total_score = 0
    total_weight = 0

    for comment in comments:
        if "sentiment_score" in comment:
            score = comment["sentiment_score"]
            weight = score.get("Positive", Decimal(0)) - score.get("Negative", Decimal(0))
            total_score += weight
            total_weight += 1

    if total_weight == 0:
        return None
    return round((total_score / total_weight + 1) * 50)

def process_record(record):
    """
    Processes a single record from the DynamoDB stream event.
    Extracts fields, performs sentiment analysis, and prepares the record for updating.
    """
    try:
        # Extract required fields
        new_image = record["dynamodb"]["NewImage"]
        video_id = new_image["video_id"]["S"]
        comment_text = new_image["comment_text"]["S"]
        unique_comment_key = new_image["unique_comment_key"]["S"]

        print(f"Processing comment: {comment_text} for video_id: {video_id}")

        # Perform sentiment analysis
        sentiment, sentiment_score = analyze_sentiment(comment_text)
        if sentiment and sentiment_score:
            print(f"Sentiment: {sentiment}, Score: {sentiment_score}")  # Debugging

            # Prepare the updated comment item
            updated_comment = {
                "unique_comment_key": unique_comment_key,
                "video_id": video_id,
                "comment_text": comment_text,
                "sentiment": sentiment,
                "sentiment_score": sentiment_score,
            }

            # Include other fields from the original record
            for key, value in new_image.items():
                if key not in updated_comment:
                    updated_comment[key] = value[list(value.keys())[0]]

            return updated_comment
    except KeyError as e:
        print(f"KeyError: Missing field in record: {e}")
    except Exception as e:
        print(f"Error processing record: {e}")

    return None

def lambda_handler(event, context):
    """
    Entry point for the Lambda function triggered by DynamoDB streams.
    """
    print(f"Received event: {json.dumps(event, indent=2)}")  # Debugging

    processed_comments = []

    # Iterate over records in the DynamoDB stream event
    for record in event.get("Records", []):
        if record["eventName"] != "INSERT":
            continue  # Process only INSERT events

        processed_comment = process_record(record)
        if processed_comment:
            processed_comments.append(processed_comment)

    # Batch update DynamoDB with analyzed comments
    if processed_comments:
        update_comments_batch(processed_comments)
        print(f"Updated {len(processed_comments)} comments.")  # Debugging

    return {
        "statusCode": 200,
        "body": json.dumps({"message": "Stream processed successfully."}),
    }
