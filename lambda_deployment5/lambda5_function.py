#!/usr/local/bin/python3.12

import sys
import os

# Ensure Lambda Layers are accessible
sys.path.append("/opt")  

import json
import boto3
from collections import Counter
from wordcloud import STOPWORDS
import numpy as np  # Try importing numpy after setting sys.path

# Initialize DynamoDB client
dynamodb = boto3.resource("dynamodb")
raw_comments_table = dynamodb.Table("RawCommentsTable")
wordcloud_table = dynamodb.Table("WordCloudTable")

def extract_meaningful_words(comments):
    """
    Extracts the top 10 most frequent meaningful words from a batch of comments.
    """
    words = []
    
    # Process each comment
    for comment in comments:
        words.extend(comment.lower().split())  # Convert to lowercase and split into words
    
    # Remove stopwords
    filtered_words = [word for word in words if word not in STOPWORDS]
    
    # Count word frequencies
    word_counts = Counter(filtered_words)
    
    # Get the top 10 most frequent words
    top_words = [word for word, count in word_counts.most_common(10)]
    
    return top_words

def lambda_handler(event, context):
    """
    AWS Lambda function triggered when a batch of comments is added to RawCommentsTable.
    Extracts top 10 meaningful words and stores them in WordCloudTable.
    """
    try:
        for record in event["Records"]:
            if record["eventName"] in ["INSERT", "MODIFY"]:  # Process only new or updated comments
                new_comment_data = record["dynamodb"]["NewImage"]
                
                video_id = new_comment_data["video_id"]["S"]
                comment_text = new_comment_data["comment_text"]["S"]  # Updated field name

                # Fetch all comments for this video_id from RawCommentsTable
                response = raw_comments_table.query(
                    KeyConditionExpression=boto3.dynamodb.conditions.Key("video_id").eq(video_id)
                )

                all_comments = [item["comment_text"] for item in response["Items"]]  # Updated field name

                # Extract meaningful words
                top_words = extract_meaningful_words(all_comments)

                # Store in WordCloudTable
                wordcloud_table.put_item(
                    Item={
                        "video_id": video_id,
                        "wordcloud": top_words
                    }
                )

        return {
            "statusCode": 200,
            "body": json.dumps("WordCloudTable updated successfully!")
        }

    except Exception as e:
        print(f"Error: {str(e)}")
        return {
            "statusCode": 500,
            "body": json.dumps("Error processing word cloud data")
        }