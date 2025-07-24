This is the README file for the application.


# Deployment:
To deploy this application, follow the steps below:

## Flask Web Server
To deploy the Flask web server, we need to create a new web app in azure portal and configure the deployment center to deploy the code from the github repository. The github workflow is used to automatically deploy the Flask web server to azure web app. The flask web server is provided in the flask folder. The website front-end is provided in the static folder.

1. Create a new web app in azure portal.
2. Go to the deployment center of the web app.
3. Choose the source as Github.
4. Choose the repository and branch to deploy.
5. Configure the workflow file as provided 
(notice that secrets are not included in the workflow file, it need to add manually in the repository settings)
6. Configure the startup command of Azure Web Application.

`wget https://johnvansickle.com/ffmpeg/releases/ffmpeg-release-amd64-static.tar.xz && \
tar -xf ffmpeg-release-amd64-static.tar.xz && \
cp ffmpeg-*/ffmpeg ffmpeg-*/ffprobe /usr/local/bin/ && \
chmod +x /usr/local/bin/ffmpeg /usr/local/bin/ffprobe && \
gunicorn app:app --bind=0.0.0.0 --timeout 600`
7. To access the website, go to the URL of the web app.
8. Add the website domain to the allowed domains of the Azure Blob Storage account to allow users to upload videos.


## Worker Service
To deploy the watermark, thumbnail, and reduce worker services, we need to create an AKS cluster and deploy the worker services to it. The watermark, thumbnail, and reduce worker are provided in scripts folders. 

1. Create AKS cluster in azure portal.
2. Install kubectl on your local machine.
3. Create a container registry in azure portal.
4. Build and push the docker images to the container registry.

    `docker buildx build --platform linux/amd64,linux/arm64 -t videoprocessregistrywatermark.azurecr.io/videoprocess-reduce-app:latest --push .`

    `docker buildx build --platform linux/amd64,linux/arm64 -t videoprocessregistrywatermark.azurecr.io/videoprocess-watermark-app:latest --push .`

    `docker buildx build --platform linux/amd64,linux/arm64 -t videoprocessregistrywatermark.azurecr.io/videoprocess-thumbnail-app:latest --push .`
5. Add the container registry to the AKS cluster.
6. Deploy the worker services to the AKS cluster using kubectl and yaml files.

    `kubectl apply -f watermark.yaml`

    `kubectl apply -f thumbnail.yaml`

    `kubectl apply -f reduce.yaml`
