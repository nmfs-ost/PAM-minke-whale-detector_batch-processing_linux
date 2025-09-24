from airflow import DAG
from airflow.providers.google.cloud.hooks.gcs import GCSHook
from airflow.exceptions import AirflowFailException
from airflow.utils.dates import days_ago
from airflow.decorators import task, dag
from google.cloud import run_v2,storage
from google.protobuf import duration_pb2
from google.api_core.client_options import ClientOptions
from google.api_core.exceptions import NotFound
from google.api import launch_stage_pb2 as ls
from google.cloud.run_v2.types import Job, TaskTemplate, VolumeMount, Volume, Condition
from google.protobuf.duration_pb2 import Duration
from google.protobuf.json_format import MessageToDict

import yaml
import logging
import os
import hashlib
import time
import datetime
import pandas as pd
from io import StringIO

def get_job_id(deployment_id,run):

    unq_str = run + deployment_id
    job_name = hashlib.sha256(unq_str.encode('utf-8')).hexdigest()[:10]
    job_id = f"nefsc-minke-detector-{job_name}"

    return job_id

# Configure logging
log = logging.getLogger(__name__)

@task
def read_yaml_from_gcs_and_xcom(**context):
    try:
        # Initialize the GCS Hook. In Cloud Composer, default authentication is handled.
        gcs_hook = GCSHook()

        log.info(f"Attempting to read gs://{context['params']['run_params_yaml_and_summary_bucket_name']}/{context['params']['run_params_yaml_path']}")

        # Read the content of the YAML file
        # The 'download_as_bytes' method is used here, then decoded to string.
        # For larger files, consider streaming or other methods if memory is an issue.
        yaml_content_bytes = gcs_hook.download_as_byte_array(bucket_name=context['params']['run_params_yaml_and_summary_bucket_name'], object_name=context['params']['run_params_yaml_path'])
        yaml_content_str = yaml_content_bytes.decode('utf-8')

        log.info("YAML content read successfully. Parsing...")

        # Parse the YAML content into a Python dictionary
        data_dict = yaml.safe_load(yaml_content_str)

        log.info("YAML content parsed into dictionary.")
        log.debug(f"Parsed dictionary: {data_dict}")

        # Push the dictionary to XCom so it can be used by downstream tasks
        context['ti'].xcom_push(key='gcs_yaml_data', value=data_dict)
        log.info("Parsed dictionary pushed to XCom with key 'gcs_yaml_data'.")

        #return the list variable of 
        return data_dict['INPUT_PATHS']

    except Exception as e:
        log.error(f"Error reading or parsing YAML from GCS: {e}")
        raise # Re-raise the exception to fail the task if something goes wrong

#def use_parsed_data_downstream(**context):
#    """
#    A dummy downstream task to demonstrate how to pull the parsed YAML data from XCom.
#    """
#    log.info("Downstream task started. Attempting to pull data from XCom.")
#    # Pull the dictionary from XCom using the same key it was pushed with
#    parsed_data = context['ti'].xcom_pull(key='gcs_yaml_data', task_ids='read_yaml_from_gcs')

#    if parsed_data:
#        log.info("Successfully pulled data from XCom:")
#        log.info(f"Type of pulled data: {type(parsed_data)}")
#        log.info(f"Content: {parsed_data}")
#    else:
#        log.warning("No data found in XCom for 'gcs_yaml_data'.")


@task
def deploy_job(run, **context):

    try:

        log.info(f"Upstream task started. Input param: {run}")
        parsed_data = context['ti'].xcom_pull(key='gcs_yaml_data', task_ids='read_yaml_from_gcs_and_xcom')

        #define variables: 

        project_id     = "ggn-nmfs-pamdata-prod-1" #os.environ['GCP_project'] #["GOOGLE_CLOUD_PROJECT"]    ?
        region         = "us-east4"
        deployment_id  = parsed_data['DEPLOYMENT_ID']
        data_path = run 
        output_path    = parsed_data["OUTPUT_PATH"]
        channel = str(parsed_data.get('CHANNEL',""))
        extension = parsed_data.get('EXTENSION',"")
        batch_size = str(parsed_data.get('BATCH_SIZE',""))
        step_sec = str(parsed_data.get('STEP_SEC',""))
        smooth_sec = str(parsed_data.get('SMOOTH_SEC',""))
        threshold = str(parsed_data.get('THRESHOLD',""))
        min_dur_sec = str(parsed_data.get('MIN_DUR_SEC',""))
        max_dur_sec = str(parsed_data.get('MAX_DUR_SEC',""))
        class_id = str(parsed_data.get('CLASS_ID',""))
        chunk_size_sec = str(parsed_data.get('CHUNK_SIZE_SEC',""))
        min_confidence = str(parsed_data.get('MIN_CONFIDENCE',""))
        recursive = str(parsed_data.get('RECURSIVE',""))
        summary_time_offset = str(parsed_data.get('SUMMARY_TIME_OFFSET',""))

        IMAGE_URI = "us-east4-docker.pkg.dev/ggn-nmfs-pamdata-prod-1/pamdata-docker-repo/nefsc-minke-detector:latest "
        
        #this is questionable. Do we want this to be composer? No, still want it to be this
        #since composer doesn't need to access / write to pamdata buckets. 
        SA="nefsc-minke-detector@ggn-nmfs-pamdata-prod-1.iam.gserviceaccount.com"

        #define and deploy job using parameter data and the variable input. 

        #return run

        #client = CloudRunHook(gcp_conn_id="google_cloud_default", region=region).get_job_client()

        #how to define this? hash input + the deployment name? Don't have access to index in here 

        job_id = get_job_id(deployment_id,run)
        parent  = f"projects/{project_id}/locations/{region}"
        job_path = f"{parent}/jobs/{job_id}"

        log.info(f"Variables extracted and job_id defined")

        #CONTAINER ENV
        env = [
        run_v2.EnvVar(name="DATA_PATH",           value=data_path),
        run_v2.EnvVar(name="DEPLOYMENT_ID",       value=deployment_id),
        run_v2.EnvVar(name="CHANNEL",             value=channel),
        run_v2.EnvVar(name="EXTENSION",           value=extension),
        run_v2.EnvVar(name="BATCH_SIZE",          value=batch_size),
        run_v2.EnvVar(name="STEP_SEC",            value=step_sec),
        run_v2.EnvVar(name="SMOOTH_SEC",          value=smooth_sec),
        run_v2.EnvVar(name="THRESHOLD",           value=threshold),
        run_v2.EnvVar(name="MIN_DUR_SEC",         value=min_dur_sec),
        run_v2.EnvVar(name="MAX_DUR_SEC",         value=max_dur_sec),
        run_v2.EnvVar(name="CLASS_ID",            value=class_id),
        run_v2.EnvVar(name="CHUNK_SIZE_SEC",      value=chunk_size_sec),
        run_v2.EnvVar(name="MIN_CONFIDENCE",      value=min_confidence),
        run_v2.EnvVar(name="RECURSIVE",           value=recursive),
        run_v2.EnvVar(name="SUMMARY_TIME_OFFSET", value=summary_time_offset),
        ]

        log.info(f"Container env defined in job deploy spec")

        #MOUNTS
        # input-volume
        in_bucket, in_dir = data_path.split("/", 1)
        input_volume = run_v2.Volume(
            name="input-volume",
            gcs=run_v2.GCSVolumeSource(
                bucket=in_bucket,
                mount_options=[f"only-dir={in_dir}"]
            )
        )

        out_bucket, out_dir = output_path.split("/", 1)
        # mimic "$(echo $OUTPUT_PATH | cut -d '/' -f2-)/$(echo $DATA_PATH | cut -d '/' -f3,4)"
        extra_path = "/".join(data_path.split("/")[2:4])
        out_dir = f"{out_dir}/{extra_path}"
        output_volume = run_v2.Volume(
            name="output-volume",
            gcs=run_v2.GCSVolumeSource(
                bucket=out_bucket,
                mount_options=[f"only-dir={out_dir}"]
            )
        )

        volume_mounts = [
            run_v2.VolumeMount(name="input-volume",  mount_path="/input"),
            run_v2.VolumeMount(name="output-volume", mount_path="/output"),
        ]

        log.info(f"Mounts defined in job deploy spec")

        #CONTAINER SPEC
        container = run_v2.Container(
            name="main",                    # arbitrary
            image=IMAGE_URI,
            env=env,
            volume_mounts=volume_mounts,
            resources=run_v2.ResourceRequirements(
                limits={"memory": "32Gi", "cpu": "8"}   # --memory / --cpu
            ),
        )

        #TASK LEVEL SETTINGS
        task_template = run_v2.TaskTemplate(
            containers=[container],
            volumes=[input_volume, output_volume],
            timeout=duration_pb2.Duration(seconds=168 * 3600),         # 168 h
            service_account=SA,
        )

        #LABELS 
        execution_template = run_v2.ExecutionTemplate(
            template=task_template,
        )

        log.info(f"Execution data defined in job deploy spec")

        job = run_v2.Job(template=execution_template,
            labels={
                "deployment-id":     deployment_id,
                "batch-process-name": "nefsc-minke-detector",
            },
            launch_stage=ls.BETA
        )

        client_opts = ClientOptions(api_endpoint=f"{region}-run.googleapis.com")
        client = run_v2.JobsClient(client_options=client_opts)

        log.info(f"Deploying Job")

        try:
            log.info(f'jobname:{job_id}')
            client.get_job(name=job_path)
            log.info(f'updating:{job_id}')

            op = client.delete_job(name=job_path)
            #request = run_v2.UpdateJobRequest(job=job)
            #op = client.update_job(request=request)
            log.info(f"Job already exists, deleting and waiting 30 seconds")
            time.sleep(30) 
        except:
            pass
        
        op = client.create_job(parent=parent, job=job, job_id=job_id)  # first deploy
        log.info(f'creating:{job_id}')
        #op.result()    # wait until the Job configuration is stored
        log.info(f"Creating new job")

        log.info(f"result:{op.result()}")

    except Exception as e:
        log.error(f"Error deploying job: {e}")
        raise 

@task()
def run_job(run,**context):

    log.info(f"Starting job run for {run}")

    parsed_data = context['ti'].xcom_pull(key='gcs_yaml_data', task_ids='read_yaml_from_gcs_and_xcom')
    deployment_id = parsed_data['DEPLOYMENT_ID']

    job_id = get_job_id(deployment_id,run)

    project_id     = "ggn-nmfs-pamdata-prod-1" 
    region         = "us-east4"

    parent  = f"projects/{project_id}/locations/{region}"

    client_opts = ClientOptions(api_endpoint=f"{region}-run.googleapis.com")
    client = run_v2.JobsClient(client_options=client_opts)

    run_op = client.run_job(name=f"{parent}/jobs/{job_id}")
    print("Execution started; ID:", run_op.metadata.name)

@task()
def wait_for_job_completion(run, poll_interval: int = 60,**context):

    log.info(f"Polling status for: {run}")

    initial_time = time.time()

    parsed_data = context['ti'].xcom_pull(key='gcs_yaml_data', task_ids='read_yaml_from_gcs_and_xcom')
    deployment_id = parsed_data['DEPLOYMENT_ID']

    job_id = get_job_id(deployment_id,run)
    
    project_id     = "ggn-nmfs-pamdata-prod-1" 
    region         = "us-east4"

    parent  = f"projects/{project_id}/locations/{region}"

    #client = run_v2.JobsClient()
    client = run_v2.ExecutionsClient()
    job_path = f"projects/{project_id}/locations/{region}/jobs/{job_id}"
   
    while True:
        execution = client.list_executions(parent=job_path)
        execution = [p for p in execution][0] #can safely assume only 1 execution

        template_dict = MessageToDict(execution.template._pb)

        completed = [i for i in execution.conditions if i.type_ == "Completed"][0]
        if completed.state == Condition.State.CONDITION_SUCCEEDED:
            log.info(f"Job {job_id} Succeeded!")
        elif completed.state == Condition.State.CONDITION_FAILED:
            log.info(f"Job {job_id} Failed.")
        
        if completed.state in (Condition.State.CONDITION_SUCCEEDED,Condition.State.CONDITION_FAILED):
            return {'run_input_dir':run,'job_name':job_id,'status':completed.state.name,'run_output_dir':[i['gcs']['bucket'] + "/" + i['gcs']['mountOptions'][0].split("only-dir=")[1] for i in template_dict["volumes"] if i['name']=='output-volume'][0],'deployment_id':deployment_id,'execution_name':execution.name,'execution_job':execution.job,'start_time':execution.create_time.isoformat(),'update_time':completed.last_transition_time.isoformat(),'execution_data':template_dict}

        latest_time = time.time()

        log.info(f"Checked {job_id} at {datetime.datetime.fromtimestamp(latest_time).strftime('%Y-%m-%d %H:%M:%S')}. Current processing time: {round(latest_time-initial_time)} seconds. Still ongoing, job state is {completed.state}")
                
        time.sleep(poll_interval)

@task()
def delete_job(output,run,**context):

    if output["status"] != "CONDITION_SUCCEEDED":
        log.info(f"{output['execution_job']},{output['run_input_dir']} had failure state, not deleting to allow for more detailed troubleshooting")
    else:
        log.info(f"{output['execution_job']},{output['run_input_dir']} completed successfully, deleting")
        
        parsed_data = context['ti'].xcom_pull(key='gcs_yaml_data', task_ids='read_yaml_from_gcs_and_xcom')
        deployment_id = parsed_data['DEPLOYMENT_ID']

        job_id = get_job_id(deployment_id,run)
        
        project_id     = "ggn-nmfs-pamdata-prod-1" 
        region         = "us-east4"

        parent  = f"projects/{project_id}/locations/{region}"
        
        client = run_v2.JobsClient()
        job_path = f"projects/{project_id}/locations/{region}/jobs/{job_id}"

        client.delete_job(name=job_path)

        log.info(f"{output['execution_job']},{output['run_input_dir']} deleted successfully")

@task()
def aggregate_artifacts(outputs,**context):

    #check that the value for the summary dir has a trailing slash, add if not
    outdir = context['params']["summary_dir"]
    outdir = outdir if outdir.endswith('/') else outdir+"/"

    outpath = f"gs://{context['params']['run_params_yaml_and_summary_bucket_name']}/{outdir}job_stats.csv"

    log.info(f"Writing run summary csv to {outpath}")

    parsed_data = context['ti'].xcom_pull(key='gcs_yaml_data', task_ids='read_yaml_from_gcs_and_xcom')

    df = pd.DataFrame(outputs)
    df["start_time"] = pd.to_datetime(df["start_time"])
    df["update_time"] = pd.to_datetime(df["update_time"])
    df["duration"] = (df["update_time"] - df["start_time"]).dt.total_seconds()

    executions_yamls = df.pop("execution_data").tolist()

    buffer = StringIO()
    df.to_csv(buffer,index=False)
    buffer.seek(0)

    client = storage.Client()
    bucket = client.bucket(context['params']['run_params_yaml_and_summary_bucket_name'])
    blob = bucket.blob(outdir+"job_stats.csv")

    blob.upload_from_string(buffer.getvalue(),content_type="text/csv")

    log.info(f"Wrote csv summary to {outpath}")

    #now write frozen params yamls

    for idx,val in enumerate(df.run_input_dir):

        val = val.replace("/","_")

        outpath2 = f"gs://{context['params']['run_params_yaml_and_summary_bucket_name']}/{outdir}yamls/job_yaml_{val}.yml"

        log.info(f"Writing run summary yaml to {outpath2}")

        yaml_string = yaml.dump(executions_yamls[idx], default_flow_style=False)

        blob = bucket.blob(outdir+f"yamls/job_yaml_{val}.yml")

        blob.upload_from_string(
                data=yaml_string,
                content_type='application/x-yaml'
            )

@task()
def zip_inputs(outputs,sources):
    return [{"output":o,"run":r} for o,r in zip(outputs,sources)]

@dag(
    dag_id='nefsc_minke_dag',
    start_date=days_ago(1),
    schedule_interval=None, # Run manually or set a schedule
    catchup=False,
    default_args={"retries":0},
    tags=['test', 'detector', 'minke','dfw','dev'],
    params={
        "run_params_yaml_and_summary_bucket_name": "pamdata-app-intermediates",
        "run_params_yaml_path": "composer/my_run/my_parameters.yml",
        "summary_dir": "composer/my_run/"
    },
    doc_md="NEFSC minke detector authored by Xavier Muoy and migrated by Dan Woodrich"
)
def dag_():

    sources = read_yaml_from_gcs_and_xcom()

    deploy_task = deploy_job.expand(run=sources)
    run_task = run_job.expand(run=sources)
    wait_task = wait_for_job_completion.expand(run=sources)
    zip_inputs_ = zip_inputs(wait_task,sources)
    delete_task = delete_job.expand_kwargs(zip_inputs_)
    agg_task = aggregate_artifacts(outputs = wait_task)

    deploy_task >> run_task >> wait_task >> delete_task >>  agg_task

dag__ = dag_()