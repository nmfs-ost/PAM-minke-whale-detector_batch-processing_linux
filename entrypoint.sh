#!/bin/bash

#needs to have unix line endings!

CMD1="python /app/run_ketos_detector.py"
CMD2="python /app/create_detection_spectrograms_and_spreadsheet.py"
CMD3="python /app/MIWH_HUWH_merge_netcdf_files.py"

[ "${RECURSIVE}" = "True" ] && CMD1+=" --recursive"
[ ! -z "${CHANNEL}" ] && CMD1+=" --channel=$CHANNEL"
[ ! -z "${EXTENSION}" ] && CMD1+=" --extension=$EXTENSION"
[ ! -z "${BATCH_SIZE}" ] && CMD1+=" --batch_size=$BATCH_SIZE"
[ ! -z "${STEP_SEC}" ] && CMD1+=" --step_sec=$STEP_SEC"
[ ! -z "${SMOOTH_SEC}" ] && CMD1+=" --smooth_sec=$SMOOTH_SEC"
[ ! -z "${THRESHOLD}" ] && CMD1+=" --threshold=$THRESHOLD"
[ ! -z "${CLASS_ID}" ] && CMD1+=" --class_id=$CLASS_ID"
[ ! -z "${CHUNK_SIZE_SEC}" ] && CMD1+=" --chunk_size_sec=$CHUNK_SIZE_SEC"
[ ! -z "${MIN_DUR_SEC}" ] && CMD1+=" --min_dur_sec=$MIN_DUR_SEC"
[ ! -z "${MAX_DUR_SEC}" ] && CMD1+=" --max_dur_sec=$MAX_DUR_SEC"

#hardcoded
CMD1+=" --model=/app/ketos_model.kt"
CMD1+=" --audio_folder=/input/"
CMD1+=" --output_folder=/output/"
#don't allow defaults for:
CMD1+=" --deployment_id=$DEPLOYMENT_ID"

ITERATION=1
while [ ! -f "donefile.txt" ]; do
  echo ITERATION:$ITERATION $CMD1
  eval $CMD1
  ((ITERATION++))
done

[ ! -z "${MIN_CONFIDENCE}" ] && CMD2+=" --min_confidence=$MIN_CONFIDENCE"
[ ! -z "${SUMMARY_TIME_OFFSET}" ] && CMD2+=" --time_offset=$SUMMARY_TIME_OFFSET"

CMD2+=" --detec_dir=/output/"

echo $CMD2
eval $CMD2

echo $CMD3
eval $CMD3





