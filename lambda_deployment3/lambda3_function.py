#!/usr/local/bin/python3.12

import json
import boto3
import os
from botocore.exceptions import ClientError
from time import sleep

# Initialize DynamoDB and Comprehend clients
dynamodb = boto3.resource('dynamodb')
comprehend = boto3.client('comprehend')

# Reference your DynamoDB table (use environment variable for table name)
table_name = os.environ.get('DYNAMODB_TABLE_NAME', 'RawCommentsTable')
table = dynamodb.Table(table_name)

def fetch_comments(video_id):
    """
    Fetch all comments for a given video ID from DynamoDB, with pagination support.
    """
    print(f"Fetching comments for video_id: {video_id}")
    comments = []
    try:
        response = table.query(
            KeyConditionExpression=boto3.dynamodb.conditions.Key('video_id').eq(video_id)
        )
        comments.extend(response['Items'])

        # Handle pagination for large datasets
        while 'LastEvaluatedKey' in response:
            response = table.query(
                KeyConditionExpression=boto3.dynamodb.conditions.Key('video_id').eq(video_id),
                ExclusiveStartKey=response['LastEvaluatedKey']
            )
            comments.extend(response['Items'])

        print(f"Fetched {len(comments)} comments for video_id: {video_id}")
        return comments
    except ClientError as e:
        print(f"Error fetching comments for video_id {video_id}: {e}")
        return []

def analyze_sentiment(text):
    """
    Use AWS Comprehend to analyze the sentiment of a comment.
    """
    try:
        response = comprehend.detect_sentiment(Text=text, LanguageCode='en')
        return response['Sentiment'], response['SentimentScore']
    except comprehend.exceptions.TextSizeLimitExceededException as e:
        print(f"Text too long for sentiment analysis: {e}")
    except comprehend.exceptions.ThrottlingException as e:
        print("Throttling error, retrying...")
        sleep(1)  # Add delay before retrying
        return analyze_sentiment(text)  # Retry the request
    except ClientError as e:
        print(f"Error analyzing sentiment: {e}")
    return None, None

def update_comments_batch(video_id, processed_comments):
    """
    Batch update comments with sentiment analysis results.
    """
    with table.batch_writer() as batch:
        for comment in processed_comments:
            batch.put_item(Item=comment)
    print(f"Batch updated {len(processed_comments)} comments for video_id: {video_id}")

def aggregate_sentiment_scores(comments):
    """
    Aggregates sentiment scores to calculate an overall sentiment score for the video.
    """
    total_score = 0
    total_weight = 0

    for comment in comments:
        if 'sentiment_score' in comment:
            score = comment['sentiment_score']  # Example: {'Positive': 0.9, 'Negative': 0.1}
            weight = score.get('Positive', 0) - score.get('Negative', 0)
            total_score += weight
            total_weight += 1

    # Return normalized score between 0 and 100
    if total_weight == 0:
        return None
    return round((total_score / total_weight + 1) * 50)

def lambda_handler(event, context):
    """
    Entry point for the Lambda function.
    """
    # Validate input
    if 'video_id' not in event:
        return {
            'statusCode': 400,
            'body': json.dumps({'error': 'Missing video_id in the event.'})
        }

    video_id = event['video_id']
    print(f"Processing video ID: {video_id}")

    # Fetch comments
    comments = fetch_comments(video_id)
    if not comments:
        return {
            'statusCode': 404,
            'body': json.dumps({'message': 'No comments found for this video ID.'})
        }

    processed_comments = []
    for comment in comments:
        if 'comment_text' not in comment or not comment['comment_text']:
            print(f"Skipping invalid comment: {comment}")
            continue
        sentiment, sentiment_score = analyze_sentiment(comment['comment_text'])
        if sentiment and sentiment_score:
            comment['sentiment'] = sentiment
            comment['sentiment_score'] = sentiment_score
            processed_comments.append(comment)

    # Batch update DynamoDB
    if processed_comments:
        update_comments_batch(video_id, processed_comments)

    # Compute and return overall sentiment score
    overall_score = aggregate_sentiment_scores(processed_comments)
    return {
        'statusCode': 200,
        'body': json.dumps({
            'message': 'Sentiment analysis completed.',
            'overall_score': overall_score
        })
    }
