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
import time

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


# https://cloudcomputinga2b022.blob.core.windows.net/video-storage/testvideo1.mov
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

def get_video_chunks_watermark(job_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT watermark_chunk_url FROM video_chunk_job WHERE job_id = %s", (job_id,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    url_list = []
    if result:
        for row in result:
            url_list.append(row[0])
        return url_list
    else:
        return None

def get_video_chunks_thumbnail(job_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT thumbnail_chunk_url FROM video_chunk_job WHERE job_id = %s", (job_id,))
    result = cur.fetchall()
    cur.close()
    conn.close()
    url_list = []
    if result:
        for row in result:
            url_list.append(row[0])
        return url_list
    else:
        return None
    
def update_video_job_status(job_id, watermark_url, thumbnail_url, status):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE video_job SET watermark_url = %s, thumbnail_url = %s, status = %s WHERE job_id = %s", (watermark_url, thumbnail_url, status, job_id))
    conn.commit()
    cur.close()
    conn.close()

def update_video_job_watermark(job_id, watermark_url):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE video_job SET watermark_url = %s WHERE job_id = %s", (watermark_url, job_id))
    conn.commit()
    cur.close()
    conn.close()

def update_video_job_thumbnail(job_id, thumbnail_url):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("UPDATE video_job SET thumbnail_url = %s WHERE job_id = %s", (thumbnail_url, job_id))
    conn.commit()
    cur.close()
    conn.close()

def get_video_job_status(job_id):
    conn = get_db_connection()
    cur = conn.cursor()
    cur.execute("SELECT watermark_url, thumbnail_url, status FROM video_job WHERE job_id = %s", (job_id,))
    result = cur.fetchone()
    cur.close()
    conn.close()
    if result:
        return result[0], result[1], result[2]
    else:
        return None, None, None 

def process_final_watermark_video(job_id):
    url_list = get_video_chunks_watermark(job_id)
    
    final_video_path = os.path.join(RESULT_FOLDER, f"{job_id}_final.mp4")
    url_list = sorted(url_list)

    # # openCV combine video
    # first_chunk_path = download_file_from_blob(url_list[0], os.path.join(DOWNLOAD_FOLDER, url_list[0]))
    # cap0 = cv2.VideoCapture(first_chunk_path)
    # fps = cap0.get(cv2.CAP_PROP_FPS)
    # width = int(cap0.get(cv2.CAP_PROP_FRAME_WIDTH))
    # height = int(cap0.get(cv2.CAP_PROP_FRAME_HEIGHT))
    # fourcc = cv2.VideoWriter_fourcc(*'mp4v')

    # out = cv2.VideoWriter(final_video_path, fourcc, fps, (width, height))
    
    
    # print(" Processing final video...")
    # for blob_name in url_list:
    #     logging.info(f"Downloading {blob_name}...")
    #     chunk_path = download_file_from_blob(blob_name, os.path.join(DOWNLOAD_FOLDER, blob_name))
    #     cap = cv2.VideoCapture(chunk_path)
    #     frame_written = 0
    #     while True:
    #         ret, frame = cap.read()
    #         if not ret:
    #             break
    #         out.write(frame)
    #         frame_written += 1
    #     logging.info(f"Wrote {frame_written} frames to {final_video_path}")
    #     cap.release()
    # out.release()

    try:
        print("Processing final video...")
        input_txt_path = f"concat_list_{job_id}.txt"
        with open(input_txt_path, "w") as f:
            for blob_name in url_list:
                chunk_path = download_file_from_blob(blob_name, os.path.join(DOWNLOAD_FOLDER, blob_name))
                f.write(f"file '{chunk_path}'\n")

        print("Concatenating video chunks...")
        import ffmpeg
        ffmpeg.input(input_txt_path, format='concat', safe=0).output(final_video_path, c='libx264',preset='fast').run()
    except Exception as e:
        print("Error processing final video: %s", e)
        logging.error('Exception:%s', str(e))
        return None

    return upload_file_to_blob(final_video_path, f"{job_id}_final.mp4")


def process_final_thumbnail(job_id):
    try: 
        url_list = get_video_chunks_thumbnail(job_id)
        final_thumbnail_path = os.path.join(RESULT_FOLDER, f"{job_id}_final_thumbnail.jpg")

        img_list = []
        for blob_name in url_list:
            image_path = download_file_from_blob(blob_name, os.path.join(DOWNLOAD_FOLDER, blob_name))
            if not image_path:
                continue
            image = Image.open(image_path)
            img_list.append(image)    

        cols = math.ceil(math.sqrt(len(url_list)))
        rows = math.ceil(len(url_list) / cols)
        thumb_width, thumb_height = img_list[0].size

        # Create canvas
        final_thumb = Image.new('RGB', (cols * thumb_width, rows * thumb_height))

        for idx, img in enumerate(img_list):
            x = (idx % cols) * thumb_width
            y = (idx // cols) * thumb_height
            final_thumb.paste(img, (x, y))
        final_thumb.save(final_thumbnail_path)
    except Exception as e:
        logging.error('Exception:', str(e))

    return upload_file_to_blob(final_thumbnail_path, f"{job_id}_final_thumbnail.jpg")

job_list = []

def reduce_task():
    try: 
        start_time = datetime.now()
        # job_id = message.get('job_id')
        # job_id = "b5acaa86-17fb-423d-b688-a96c79c45c10"
        

        message_str = get_message("video-reduce")
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
        
        if job_id in job_list:
            print("Job %s is already processing", job_id)
            return
        job_list.append(job_id)

        print("start processing job %s", job_id)

        from concurrent.futures import ThreadPoolExecutor

        with ThreadPoolExecutor() as executor:
            future1 = executor.submit(process_final_watermark_video, job_id)
            future2 = executor.submit(process_final_thumbnail, job_id)
        
            watermark_url = future1.result()
            thumbnail_url = future2.result()  

        # watermark_url = process_final_watermark_video(job_id)
        # thumbnail_url = process_final_thumbnail(job_id)  
        
        if not watermark_url and not thumbnail_url:
            print("no result found")
            return

        end_time = datetime.now()
        duration = (end_time - start_time).total_seconds()
        print("update job status to finished, %s", duration)

        print("Watermark URL: %s, thumbnail URL: %s", watermark_url, thumbnail_url)

        if watermark_url and thumbnail_url:
            update_video_job_status(job_id, watermark_url, thumbnail_url, "finished")
        elif watermark_url:
            update_video_job_watermark(job_id, watermark_url)
        elif thumbnail_url:
            update_video_job_thumbnail(job_id, thumbnail_url)
        
        if os.path.exists(os.path.join(RESULT_FOLDER, f"{job_id}_final.mp4")):
            os.remove(os.path.join(RESULT_FOLDER, f"{job_id}_final.mp4"))
        if os.path.exists(os.path.join(RESULT_FOLDER, f"{job_id}_final_thumbnail.jpg")):
            os.remove(os.path.join(RESULT_FOLDER, f"{job_id}_final_thumbnail.jpg"))

        for blob_name in get_video_chunks_watermark(job_id):
            if os.path.exists(os.path.join(DOWNLOAD_FOLDER, blob_name)):
                os.remove(os.path.join(DOWNLOAD_FOLDER, blob_name))
        for blob_name in get_video_chunks_thumbnail(job_id):
            if os.path.exists(os.path.join(DOWNLOAD_FOLDER, blob_name)):
                os.remove(os.path.join(DOWNLOAD_FOLDER, blob_name))
    except Exception as e:
        logging.error(e)
    # time.sleep(10)

if __name__ == '__main__':
    # reduce_task()
    # import time
    while True:
        reduce_task()
        time.sleep(10)