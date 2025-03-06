FROM python:3.9

WORKDIR /app

#copy and run python requirements
COPY /scripts/requirements.txt /app
RUN pip install -r requirements.txt

#copy application files
COPY /scripts/run_ketos_detector.py /app
COPY rclone.conf /app
COPY /models/spectro-5s_fft-0.256_step-0.03_fmin-0_fmax-800_no-norm/ketos_model.kt /app
COPY /scripts/create_detection_spectrograms_and_spreadsheet.py /app
COPY entrypoint.sh /app

#allow for execution of entrypoint script
RUN chmod +x /app/entrypoint.sh

#make dirs that app / rclone expects
RUN mkdir ./tmp/

ENTRYPOINT ["/app/entrypoint.sh"]
