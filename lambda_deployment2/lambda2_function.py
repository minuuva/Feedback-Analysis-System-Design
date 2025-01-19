#!/usr/local/bin/python3.12
import json
import boto3
import hashlib
from datetime import datetime

# Initialize the DynamoDB resource
dynamodb = boto3.resource('dynamodb')

# Reference the DynamoDB table
table = dynamodb.Table('RawCommentsTable')

def process_message(message):
    """
    Processes a single SQS message and writes the comments to DynamoDB.
    """
    try:
        # Parse the SQS message body (body contains the actual message sent to SQS)
        body = json.loads(message['body'])  # Use 'body' (lowercase) instead of 'Body'
        
        # Validate the structure of the message
        if 'video_id' not in body or 'comments' not in body:
            raise ValueError("Invalid message format: Missing 'video_id' or 'comments'.")

        video_id = body['video_id']
        comments = body['comments']

        # Ensure 'comments' is a list
        if not isinstance(comments, list):
            raise ValueError("Invalid message format: 'comments' should be a list.")

        # Write each comment to DynamoDB
        for comment in comments:
            # Generate a unique key for the comment using video_id and published_at
            unique_comment_key = hashlib.sha256(
                f"{video_id}_{comment.get('published_at', 'N/A')}".encode()
            ).hexdigest()

            # Prepare the DynamoDB item
            item = {
                'video_id': video_id,  # Partition key
                'unique_comment_key': unique_comment_key,  # Sort key
                'comment_text': comment.get('text', 'N/A'),
                'author': comment.get('author', 'Anonymous'),
                'published_at': comment.get('published_at', 'N/A'),
                'processed_at': datetime.utcnow().isoformat(),  # Add processing timestamp
            }

            # Insert the item into DynamoDB
            table.put_item(Item=item)

        print(f"Successfully processed message for video_id: {video_id}")
        return True
    except Exception as e:
        print(f"Error processing message: {e}")
        return False


def lambda_handler(event, context):
    """
    Lambda entry point. Processes messages from SQS and writes them to DynamoDB.
    """
    try:
        # Log the entire event for debugging
        print(f"Received event: {json.dumps(event, indent=2)}")
        
        for record in event['Records']:
            success = process_message(record)
            if not success:
                print("Failed to process a message.")

        return {
            'statusCode': 200,
            'body': json.dumps({'message': 'Messages processed successfully.'})
        }
    except Exception as e:
        print(f"Error in Lambda handler: {e}")
        return {
            'statusCode': 500,
            'body': json.dumps({'error': str(e)})
        }