# Minke whale detector (batch-linux version)
This is a version of the minke whale detector running in batch mode (with processing queue) in a linux environment.

The branch 'docker' takes the needed logic of the minke whale detector and delivers it in a container form. The same container image is runnable on GCP cloud run as well as docker compose. It uses a minimal # of files from the upstream repo and discards most of the environment and parameter passing since this is handled in a more standard way within the docker build process as well as GCP cloud run / docker compose for parameter passing. The container assumes that mounts have been preconfigured prior to runtime: internally uses /input and /output. 

To use this container with docker compose, when in this cloned repo (in a docker configured host) type 'docker compose up'. 

An example of using this container in Cloud Run is in https://github.com/nmfs-ost/PAM-Cloud/blob/main/code/use_minke_detector_ex.sh



