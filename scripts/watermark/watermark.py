import logging
import json
import mysql.connector
import cv2
import math
from PIL import Image
import os, uuid
from azure.identity import DefaultAzureCredential
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import numpy as np
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
# sas_token  = "sp=racwdl&st=2025-06-21T18:53:56Z&se=2025-06-30T02:53:56Z&sv=2024-11-04&sr=c&sig=J%2BmLtpHBRiFxRU4zIbVfTHLJV%2B1v%2FMVfUkP%2F7ELWgZw%3D"
sas_token  = "sp=racwdli&st=2025-07-23T08:48:37Z&se=2025-09-10T17:03:37Z&sv=2024-11-04&sr=c&sig=%2F3CxIztjOIVlL3%2FNh%2BCpHkueGSeessBMlBExMbRJbjE%3D"

blob_service_client = BlobServiceClient(account_url, credential=sas_token)

CONTAINER_NAME = "video-storage"

def upload_file_to_blob(local_path, blob_name):
    try:
        
        # Create a blob client using the local file name as the name for the blob
        blob_client = blob_service_client.get_blob_client(container=CONTAINER_NAME, blob=blob_name)

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
            # try:
            #     message = json.loads(message)
            # except Exception as ex:
            #     print("Error parsing message: %s", ex)
            #     logging.error("Error parsing message: %s", ex)
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

def update_watermark_status(job_id, chunk_id, blob_name, status):
    try:
        conn = get_db_connection()
        cursor = conn.cursor()
        query = "UPDATE video_chunk_job SET watermark_status = %s, watermark_chunk_url = %s WHERE job_id = %s AND chunk_id = %s"
        cursor.execute(query, (status, blob_name, job_id, chunk_id))
        conn.commit()
        cursor.close()
        conn.close()
    except Exception as ex:
        print("Error updating watermark status: %s", ex)
        logging.error("Error updating watermark status: %s", ex)

def perform_watermark(video_file, watermark_img, output_file):
    video = cv2.VideoCapture(video_file)
    fps = video.get(cv2.CAP_PROP_FPS)
    width = int(video.get(cv2.CAP_PROP_FRAME_WIDTH))
    height = int(video.get(cv2.CAP_PROP_FRAME_HEIGHT))

    try: 
        watermark = cv2.imread(watermark_img, cv2.IMREAD_UNCHANGED) 
        # watermark = cv2.cvtColor(watermark, cv2.COLOR_BGRA2RGBA)
        watermark = cv2.resize(watermark, (200, 200))  
        watermark = Image.fromarray(cv2.cvtColor(watermark, cv2.COLOR_BGR2RGB)).convert("RGBA")

        fourcc = cv2.VideoWriter_fourcc(*'mp4v')
        out = cv2.VideoWriter(output_file, fourcc, fps, (width, height))

        while True:
            ret, frame = video.read()
            if not ret:
                break
            
            frame_image = Image.fromarray(cv2.cvtColor(frame, cv2.COLOR_BGR2RGB)).convert("RGBA")

            watermark_layer = Image.new("RGBA", frame_image.size, (0, 0, 0, 0))
            watermark_layer.paste(watermark, (100, 100), mask=watermark)

            # Composite the layers
            watermarked_image = Image.alpha_composite(frame_image, watermark_layer).convert("RGBA")
            # watermarked_image.convert("RGB").save(buf, format='JPEG')
            final_frame = cv2.cvtColor(np.array(watermarked_image), cv2.COLOR_RGB2BGR)

            out.write(final_frame)
        video.release()
        out.release()
    except Exception as ex:
        logging.error('Exception: %s', str(ex))
        return None
    return output_file
    

def watermark_video():

    try:
        

        # watermark_img_path = os.path.join(DOWNLOAD_FOLDER, f"watermark.png")
        # if not os.path.exists(watermark_img_path):
        #     download_file_from_blob("watermark.png", watermark_img_path)
        
        # watermark = Image.open(watermark_img_path)
        
        # video_file_path = os.path.join(DOWNLOAD_FOLDER, f"{job_id}_{chunk_id}.mp4")
        # download_file_from_blob(blob_name, video_file_path)

        # perfrom watermarking
        start_time = datetime.now()

        message_str = get_message("watermark")
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

        watermark_img_path = os.path.join(DOWNLOAD_FOLDER, f"watermark.png")
        if not os.path.exists(watermark_img_path):
            download_file_from_blob("watermark.png", watermark_img_path)
        
        video_file_path = os.path.join(DOWNLOAD_FOLDER, f"{job_id}_{chunk_id}.mp4")
        download_file_from_blob(blob_name, video_file_path)

        # perfrom watermarking
        output_file_path = os.path.join(RESULT_FOLDER, f"{job_id}_{chunk_id}_watermark.mp4")
        perform_watermark(video_file_path, watermark_img_path, output_file_path)

        if not os.path.exists(output_file_path):
            logging.error(f"Watermarking failed for {job_id}_{chunk_id}")
            return
        
        # upload watermarked video to blob storage
        watermark_blob_name = f"{job_id}_{chunk_id}_watermark.mp4"
        upload_file_to_blob(output_file_path, watermark_blob_name)
        update_watermark_status(job_id, chunk_id, watermark_blob_name, "finished")
        
        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print(f"Watermarking time for {job_id}_{chunk_id}: {duration} seconds")
        logging.info(f"Watermarking completed for {job_id}_{chunk_id}, add to {watermark_blob_name}")
        
        os.remove(video_file_path)
        os.remove(output_file_path)
        os.remove(watermark_img_path)

    except Exception as ex:
        logging.error('Exception:', str(ex))

if __name__ == '__main__':
    import time
    while True:
        watermark_video()
        time.sleep(5)