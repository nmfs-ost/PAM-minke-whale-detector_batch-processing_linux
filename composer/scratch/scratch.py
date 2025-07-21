from google.cloud import run_v2
from google.cloud.run_v2.types import Condition
from google.protobuf.json_format import MessageToDict
#import time
#import datetime

#datetime.datetime.fromtimestamp(time.time()).strftime('%Y-%m-%d %H:%M:%S')

project_id = "ggn-nmfs-pamdata-prod-1"
region = "us-east4"
job_id = "nefsc-minke-detector-08f2a88c21"

client = run_v2.ExecutionsClient()
job_path = f"projects/{project_id}/locations/{region}/jobs/{job_id}"

executions = client.list_executions(parent=job_path)

#[(i.type_,i.state) for i in [p.conditions for p in executions][0]]
#and i.state == [p.conditions for p in executions][0]

#completed = [i for i in [p.conditions for p in executions][0] if i.type_ == "Completed" ][0]

#for i,val in  enumerate(executions):
 #   for p in val.conditions:

#        print('NEW ONE')
##        print(p)

import code
code.interact(local=dict(globals(), **locals()))