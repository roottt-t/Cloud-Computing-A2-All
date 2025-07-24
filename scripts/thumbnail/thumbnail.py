import logging
import json
import mysql.connector
import cv2
import math
from PIL import Image
import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
from azure.storage.queue import QueueClient
from azure.storage.queue import generate_queue_sas, QueueSasPermissions
import base64
from datetime import datetime
from datetime import timedelta

UPLOAD_FOLDER = 'uploads'
RESULT_FOLDER = 'results'
DOWNLOAD_FOLDER = 'downloads'
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
os.makedirs(RESULT_FOLDER, exist_ok=True)
os.makedirs(DOWNLOAD_FOLDER, exist_ok=True)

from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient

account_url = "https://cloudcomputinga2b022.blob.core.windows.net"
sas_token  = "sp=racwdl&st=2025-06-21T18:53:56Z&se=2025-06-30T02:53:56Z&sv=2024-11-04&sr=c&sig=J%2BmLtpHBRiFxRU4zIbVfTHLJV%2B1v%2FMVfUkP%2F7ELWgZw%3D"

blob_service_client = BlobServiceClient(account_url, credential=sas_token)

CONTAINER_NAME = "video-storage"

def upload_file_to_blob(local_path, blob_name):
    try:
        
        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

        # Upload the created file
        with open(local_path, "rb") as data:
            blob_client.upload_blob(data)
        return blob_client.url

    except Exception as ex:
        logging.error('Exception:', str(ex))
        return None


def download_file_from_blob(blob_name, local_path):
    try:
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)
        with open(local_path, "wb") as f:
            f.write(blob_client.download_blob().readall())
        return local_path
    except Exception as ex:
        logging.error('Exception:', str(ex))
        return None

def get_message(job_type):
    if job_type == "watermark":
        queue_name = "video-watermark-queue"
    elif job_type == "thumbnail":
        queue_name = "video-thumbnail-queue"
    elif job_type == "video-reduce":
        queue_name = "video-reduce-queue"
    elif job_type == "video":
        queue_name = "video-queue"
    else:
        print("Invalid job type")
        return

    try:
        account_name = "cloudcomputinga2b022"
        # account_key = "Nq5zBxGmEhot6oVkZgm//KU5jnXArRWN1iubxmWEYN5zZD548JolKEq+/crb1KnNkon2t8JDbUYR+AStWNcSDQ=="  
        account_key = "AEUhZSc2+hCfyU39gHNRV4vELCMlEsM3ixLL4jrZXSeNRZ9R0WEzWUd4ZOrKhdvvCEQiRaGsEvIB+ASts4UTgA=="

        sas_token = generate_queue_sas(
            account_name=account_name,
            queue_name=queue_name,
            account_key=account_key,
            # permission=QueueSasPermissions(read=True, add=True, update=True, process=True),
            expiry=datetime.utcnow() + timedelta(days=60),
            policy_id="mytesttoken"  # Identifier in Access Policies
        )
        account_url = "https://cloudcomputinga2b022.queue.core.windows.net"
        # default_credential = DefaultAzureCredential()

        # Create the QueueClient object
        queue_client = QueueClient(account_url, queue_name=queue_name ,credential=sas_token)
        
        message_list = queue_client.receive_messages()
        # print("Received messages from queue %s", message_list)
        if not message_list:
            print("No message found in queue %s", queue_name)
            logging.info("No message found in queue %s", queue_name)
            return None
        for message_str in message_list:
            base64_content = message_str.content
            decoded_bytes = base64.b64decode(base64_content)
            message = decoded_bytes.decode('utf-8')
            queue_client.delete_message(message_str)

            return message
        

    except Exception as ex:
        logging.error("Error Received message from queue: %s", ex)
        return None    

def get_db_connection():
    return mysql.connector.connect(
        user="khffsatzcd", 
        password="$oRtoRVsVKuScBF2", 
        host="videobackend-server.mysql.database.azure.com", 
        port=3306, 
        database="videobackend-database",
        ssl_ca="ssl_ca.crt.pem", 
        ssl_disabled=False
    )


def update_thumbnail_status(job_id, chunk_id, thumbnail_url, status):
    conn = get_db_connection()
    cursor = conn.cursor()
    query = "UPDATE video_chunk_job SET thumbnail_chunk_url = %s, thumbnail_status = %s WHERE job_id = %s AND chunk_id = %s"
    cursor.execute(query, (thumbnail_url, status, job_id, chunk_id))
    conn.commit()
    cursor.close()
    conn.close()


def thumbnail_video():

    try:
        # get message from queue
        # job_id = "4b55224c-4a5b-4722-8b92-b87dbfff5781"
        # chunk_id = 3
        # blob_name = f"{job_id}_{chunk_id}.mp4"

        message_str = get_message("thumbnail")
        if not message_str:
            return
        print("Received message: %s", message_str)
        try :
            message = json.loads(message_str)
        except Exception as ex:
            print("Error parsing message: %s", ex)
            logging.error("Error parsing message: %s", ex)
            return
        job_id = message["job_id"]
        chunk_id = message["chunk_id"]
        blob_name = message["blob_name"]

        video_file_path = os.path.join(DOWNLOAD_FOLDER, f"{job_id}_{chunk_id}.mp4")
        download_file_from_blob(blob_name, video_file_path)

        # create thumbnail of first frame
        video = cv2.VideoCapture(video_file_path)
        ret, frame = video.read()
        if not ret:
            logging.error(f"Failed to read frame from {video_file_path}")
            return
        
        # resize first frame to 256x256
        first_frame = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB))
        size = (256, 256)
        thumnail_image = first_frame.copy()
        thumnail_image.thumbnail(size)

        thumbnail_img_path = f"{RESULT_FOLDER}/{job_id}_{chunk_id}_thumbnail.jpg"
        thumnail_image.save(thumbnail_img_path)
        
        # upload thumbnail to blob storage
        thumbnail_blob_name = f"{job_id}_{chunk_id}_thumbnail.jpg"
        upload_file_to_blob(thumbnail_img_path, thumbnail_blob_name)
        update_thumbnail_status(job_id, chunk_id, thumbnail_blob_name, 'finished')

        video.release()
        os.remove(thumbnail_img_path)
 
    except Exception as ex:
        logging.error('Exception:', str(ex))
        os.remove(f"{RESULT_FOLDER}/{chunk_id}.jpg")


if __name__ == '__main__':
    import time
    while True:
        thumbnail_video()
        time.sleep(5)