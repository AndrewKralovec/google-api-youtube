import http.client
import httplib2
import os
import random
import time
from datetime import datetime
from apiclient.discovery import build
from apiclient.errors import HttpError
from apiclient.http import MediaFileUpload
from oauth2client.file import Storage

#region API Constints  
# These values are provided by default in the google docs, may or may not be needed, need to research!
DEFAULT_API_OPTS = {
    'auth_host_name':'localhost',
    'auth_host_port':[8080, 8090],
    'logging_level':'ERROR',
    'noauth_local_webserver':False
}

# So not really needed, since i think it will always be public, but we might update this
PRIVACY_STATUS = {
    'public': 'public',
    'private': 'private',
    'unlisted': 'unlisted'
}

# Tell the underlying HTTP transport library not to retry
httplib2.RETRIES = 1

# OAuth 2.0 refresh token storage location
OAUTH_UPLOAD_CREDENTIALS = 'oauth2-upload.json'
OAUTH_PLAYLIST_CREDENTIALS = 'oauth2-playlist.json'

# Setting chunksize equal to -1 in the code below means that the entire
# file will be uploaded in a single HTTP request
UPLOAD_CHUNKSIZE = -1

# Maximum number of times to retry the upload
MAX_RETRIES = 10

# Always retry when these exceptions are raised
RETRIABLE_EXCEPTIONS = (httplib2.HttpLib2Error, IOError, http.client.NotConnected,
  http.client.IncompleteRead, http.client.ImproperConnectionState,
  http.client.CannotSendRequest, http.client.CannotSendHeader,
  http.client.ResponseNotReady, http.client.BadStatusLine)

# Always retry when one of these status codes is raised
RETRIABLE_STATUS_CODES = [500, 502, 503, 504]

# The OAuth 2.0 access scope for youtube service
YOUTUBE_API_SERVICE_NAME = 'youtube'
YOUTUBE_API_VERSION = 'v3'
YOUTUBE_API_RESOURCE_LIST = 'snippet'
PLAYLIST_ID = ''
#endregion

def video_upload_request(options):
    """
    Create the video request body
    """
    details_body = {
        'snippet': {
            'title': options['title'],
            'description': options['description'],
            'tags': None,
            'categoryId':options['category']
        }, 
        'status' : {
            'privacyStatus': PRIVACY_STATUS['public']
        }
    }
    media_body = MediaFileUpload(options['file'], chunksize=UPLOAD_CHUNKSIZE, resumable=True)
    return (details_body, media_body)

def playlist_insert_request(video_id):
    """
    Create the playlist video insert request body
    Takes video_id as an argument, playlist_id is constant for now
    """
    return {
        'snippet': {
            'playlistId':PLAYLIST_ID,
            'resourceId':{
                'kind':'youtube#video',
                'videoId': video_id
            }
        }
    }

def attempt_upload(insert_request):
    """
    Attempt to upload the video to youtube, with a max attempt limit of 10, MAX_RETRIES
    """
    response, error, attempt = (None, None, 0)
    while response is None:
        try:
            status, response = insert_request.next_chunk()
            if 'id' not in response:                
                error =  "The upload failed with an unexpected response: %s with status %s" % (response, status)

        except HttpError as e:
            if e.resp.status in RETRIABLE_STATUS_CODES:
                error = "HTTP error %d occurred:\n%s\nCan retry" % (e.resp.status, e.content)
            else:
                ## Should not re-try
                raise
            
        except RETRIABLE_EXCEPTIONS as e:
            error = "http_client error: %s\nCan retry" % e

        if error is not None:
            attempt += 1
            if attempt > MAX_RETRIES:
                break
            # Sleep and retry 
            sleep_seconds = random.random() * (2 ** attempt)
            time.sleep(sleep_seconds)

    return (error, response)

def authenticate_youtube_resource(credentials_path):
    """
    Authenticate the client and return the youtube api service 
    """
    storage = Storage(credentials_path)
    credentials = storage.get()
    if credentials is None:
        # Could not get refresh token
        raise ValueError('Invalid credentials')

    return build(YOUTUBE_API_SERVICE_NAME, YOUTUBE_API_VERSION,
        http=credentials.authorize(httplib2.Http()))

def youtube_playlist_insert(props):
    error, response = (None, None)
    try:
        youtube_resource = authenticate_youtube_resource(OAUTH_PLAYLIST_CREDENTIALS)
        response = youtube_resource.playlistItems().insert(
            body=props,
            part=YOUTUBE_API_RESOURCE_LIST
        ).execute()
    except Exception as e:
        error = e

    return (error, response)

def youtube_upload_service(options):
    """
    Authenticate the youtube resource and create a video request from the options
    The request is made and returns the error/response from the upload
    """
    try:
        youtube_resource = authenticate_youtube_resource(OAUTH_UPLOAD_CREDENTIALS)
        body, media_body  = video_upload_request(options)
        video_request = youtube_resource.videos().insert(
            part=",".join(list(body.keys())),
            body=body,
            media_body=media_body
        )
        return attempt_upload(video_request)
    except HttpError as error:
        return (error, None)
