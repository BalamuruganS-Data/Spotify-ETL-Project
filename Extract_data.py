import json
import os
import spotipy
from spotipy.oauth2 import SpotifyClientCredentials
import boto3
from datetime import datetime


def lambda_handler(event, context):
    ##Spotify API
    cilent_id = os.environ.get('client_id')
    client_secret = os.environ.get('client_secret')
    
    client_credentials_manager = SpotifyClientCredentials(client_id=cilent_id, client_secret=client_secret)
    sp = spotipy.Spotify(client_credentials_manager = client_credentials_manager)
    
    ##Extract playlist data
    
    playlists = sp.user_playlists('spotify')
    playlist_link = "https://open.spotify.com/playlist/71PSYf0Nrc0XgUenDY1yXo?si=053928b328a74979"
    playlist_URI = playlist_link.split("/")[-1].split("?")[0]
    
    spotify_data = sp.playlist_tracks(playlist_URI)   
    cilent = boto3.client('s3')
    filename = "spotify_raw_data" + str(datetime.now()) + ".json"
    
    ## Save data into this folder
    cilent.put_object(
        Bucket="spotify-data-engineering-project",
        Key="discover_weekly/raw_data/to_process/" + filename,
        Body=json.dumps(spotify_data)
        )
    ses_client = boto3.client('ses')
    
    # Replace with your verified email address
    sender_email = os.environ['SENDER_EMAIL']  
    recipient_email = os.environ['RECIPIENT_EMAIL']  
    
    # Email content
    subject = "Event Notification - Spotify Extraction is Starting"
    body_text = "Hello, this is a reminder that your Spotify extraction is starting."
    
    # Send email via SES
    response = ses_client.send_email(
        Source=sender_email,
        Destination={
            'ToAddresses': [recipient_email]
        },
        Message={
            'Subject': {'Data': subject},
            'Body': {'Text': {'Data': body_text}}
        }
    )
    
    return {
        'statusCode': 200,
        'body': json.dumps(f"Email sent to {recipient_email}")
    }
