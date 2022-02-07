# startup script
# papermill /home/jupyter/script-storage/GCP/long_running_vm.ipynb long_running_vm_out.ipynb
#ong_running_vm_out.ipynb
from googleapiclient import discovery
import pandas as pd
import re 
import string
from datetime import datetime, timedelta
import time
from google.cloud import monitoring_v3
from google.cloud import pubsub_v1

# Model cloud function
def long_running_vms(project, cloud_function):
    long_running_df, instance_df, machineType_df = project_vms(project)
    send_emails(long_running_df=long_running_df, project_id=project, topic_id='email-notifications', )
    if cloud_function:
        return 
    else:
        return long_running_df, instance_df, machineType_df  # for notebook testing

# Get list of zones in project
def zone_list(service, project):
    zoneList = []
    request = service.zones().list(project=project)
    while request is not None:
        response = request.execute()
        for zone in response['items']:
            zoneList.append(zone['name'])
        request = service.zones().list_next(previous_request=request, previous_response=response)
    return request, zoneList

# Gets machine types in all zones and add to a data frame
def machine_type(service, project, zoneList):
    machineTypeList = []
    for z in zoneList:    
        request = service.machineTypes().list(project=project, zone=z)
        while request is not None:
            response = request.execute()
            for machine in response['items']:    
                machineTypeList.append([machine['zone'],machine['name'],machine['guestCpus']])
            request = service.machineTypes().list_next(previous_request=request, previous_response=response)
        
    machineType_df = pd.DataFrame(machineTypeList, columns=['zone','machinetype','cpus'])
    return machineType_df

# use the metric/machine query service to find the avg CPU utilization for all instances in the project
def cpu_utilization(project):
    client = monitoring_v3.MetricServiceClient()
    project_name = f"projects/{project}"
    now = time.time()
    seconds = int(now)
    nanos = int((now - seconds) * 10 ** 9)
    interval = monitoring_v3.types.TimeInterval()
    interval.end_time.seconds = seconds
    interval.end_time.nanos = nanos
    interval.start_time.seconds = (seconds - 3600)
    interval.start_time.nanos = nanos
 
    aggregation = monitoring_v3.types.Aggregation()
    aggregation.alignment_period.seconds =  21600
    aggregation.per_series_aligner = (monitoring_v3.enums.Aggregation.Aligner.ALIGN_MEAN) # last 6 hours
    results = client.list_time_series(
        project_name,
        'metric.type = "compute.googleapis.com/instance/cpu/utilization"',
        interval,
        monitoring_v3.enums.ListTimeSeriesRequest.TimeSeriesView.FULL,
        aggregation)
    instance_cpu_avg = []
    for result in results:
        instance_name = result.metric.labels['instance_name']
        cpu_avg_6hr = str(result.points[0].value)
        cpu_avg_6hr = float(cpu_avg_6hr[cpu_avg_6hr.find(' ')+1:len(cpu_avg_6hr)])  
        instance_cpu_avg.append([instance_name, cpu_avg_6hr])
    return dict(instance_cpu_avg)

# email processing functions

# build a string of valid email characters
email_characters = list(string.ascii_lowercase + string.ascii_uppercase + '@.-1234567890') 

# All occurrences of substring in string 
find_all = lambda meta_str, meta_sub: [iloc for iloc in range(len(meta_str)) if meta_str.startswith(meta_sub, iloc)]

# remove none email chars and return list of valid characters
pop_none_email_chars = lambda email_str: [email_chr for email_chr in email_str if email_chr in email_characters]

# parse an email string and reassemble it as a string
parse_email_str = lambda list_str, dummy_str='': dummy_str.join(map(str,pop_none_email_chars(list_str)))

# pop everything prior to the word 'value' to handle large key value pairs
def drop_word_value(email_str):
    email_ret_str = email_str
    value_locs = find_all(email_str,'value')
    for value_loc in value_locs:
        email_ret_str = email_str[value_loc+5:len(email_str)]
    return email_ret_str

# function will return either the owners email if it is populated
# or search the metadata for every occurence of an email and concate it into 
# a string of emails separated by ;
def retrieve_emails(metadata):
    emails = ''
    ownerloc = metadata.find('contact')  # find the contact (owner) tag if it exists
    if ownerloc != -1:  # owner found
        emailend=metadata[ownerloc:].find('@amfam.com')+ownerloc  # find domain following the owner location, add offset 
        emails = drop_word_value(parse_email_str(metadata[ownerloc:emailend])) + '@amfam.com'  # one email in this case
    else: # owner tage not found, search for domain name and email combo
        domainnamelocs = find_all(metadata, '@amfam.com')
        emailtaglocs = find_all(metadata, 'mail')
        for emailtagloc, domainloc in list(zip(emailtaglocs,domainnamelocs)):
            if len( drop_word_value(parse_email_str(metadata[emailtagloc+4:domainloc]))) > 0:
                emails=emails + drop_word_value(parse_email_str(metadata[emailtagloc+4:domainloc]))+'@amfam.com'  #add 4 to drop word mail
    # print(emails)
    return emails #string of emails separated by ;

# Loops through the zones, and gets all instances in each. Adds to a data frame
def populate_instance_df(service, project, zoneList, dict_instance_cpu_avg):
    instanceList = []
    for z in zoneList:    
        request = service.instances().list(project=project, zone=z)
        while request is not None:
            response = request.execute()
            if 'items' in response:
                instances = response['items']
                for instance in instances:
                    name = instance['name']
                    status = instance['status']
                    fullMachinetype = instance['machineType']
                    machineType = fullMachinetype.split('/')[-1]
                    cpuType = machineType.split('-')[0]
                    creationTimestamp = instance['creationTimestamp']
                    metadata = str(instance['metadata'])
                    emails = retrieve_emails(metadata)
                    try:  # start and stop timestamps don't always exists, when they do replace the T with a blank and strip off trailing junk
                        lastStartTimestamp = instance['lastStartTimestamp'].replace('T', ' ')[0:23]
                        lastStopTimestamp = instance['lastStopTimestamp'].replace('T', ' ')[0:23]
                    except:
                        lastStartTimestamp = None 
                        lastStopTimestamp = None
                    if lastStartTimestamp is not None and lastStopTimestamp is not None:
                        startNowDiff = datetime.now() - datetime.strptime(lastStartTimestamp, '%Y-%m-%d %H:%M:%S.%f')
                        if  datetime.strptime(lastStartTimestamp, '%Y-%m-%d %H:%M:%S.%f') < datetime.now() + timedelta(days=-1):
                            longRunning = True
                        else:
                            longRunning = False
                    else:
                        startNowDiff = None 
                        longRunning = None
                    try: cpuUsage = float(dict_instance_cpu_avg[name]) * 100
                    except: cpuUsage = 0.00
                    instanceList.append([z, name, status, machineType, cpuType, creationTimestamp, lastStartTimestamp, lastStopTimestamp, 
                                         startNowDiff, cpuUsage, emails, longRunning])
            request = service.instances().list_next(previous_request=request, previous_response=response)
    instance_df = pd.DataFrame(instanceList, columns=['zone','vmName','status','machinetype','cputype', 'çreationTimestamp',
                                                      'lastStartTimestamp', 'lastStopTimestamp', 'timeSinceLastStart',
                                                      'cpuUsagePercent','emails','longRunning'])
    return instance_df

# returns a dataframe of long running vms
def project_vms(project):
    service = discovery.build('compute', 'beta')
    request, zoneList = zone_list(service,project)
    machineType_df = machine_type(service,project,zoneList)
    dict_instance_cpu_avg = cpu_utilization(project)
    instance_df = populate_instance_df(service,project, zoneList, dict_instance_cpu_avg)
    long_running_df = instance_df.query('status == "RUNNING" and cpuUsagePercent < 1.0 and longRunning==True and vmName!={}'.format(EXCEPTION_VM)
    return long_running_df, instance_df, machineType_df

# To trigger the notification send a pub/sub message of the Project Number to:projects/gcp-ent-auto-ds-dev/topics/email-notifications
# Navigate to Cloud Storage and download email_notifications.csv which is stored at:
# gs://ent-auto-ds-emailnotification/email_notifications.csv
def send_emails(long_running_df, project_id=PROJECT_ID, topic_id="email-notifications"):
    publisher = pubsub_v1.PublisherClient()
    topic_path = publisher.topic_path(project_id, topic_id)
    for row in range(0,long_running_df.shape[0]):  # email, subject, body separated by |
        if len(long_running_df.iloc[row].emails)>0: 
            data= f'Email Notification:7' + "|" + long_running_df.iloc[row].emails + ",{}".format(EMAIL_ADDR)   # emails
        else:
            data= f'Email Notification:7' + "|{}".format(EMAIL_ADDR)   # default to EMAIL_ADDR if no contact email
        data+="|Long Running VM:"+long_running_df.iloc[row].vmName  # subject
        data+= "|VM:"+ long_running_df.iloc[row].vmName + " has been running for " # body
        data+= str(long_running_df.iloc[row].timeSinceLastStart) + " with avg CPU usage in last 6 hours of " + str(long_running_df.iloc[row].cpuUsagePercent)+"%" # body
        data = data.encode("utf-8") # Data must be a bytestring
        confirm_send = publisher.publish(topic_path, data) # Add two attributes, origin and username, to the message
        print(data)
        print(confirm_send.result())
        print(f"Published messages with custom attributes to {topic_path}.")
    if len(long_running_df) == 0:
        data = f'Email Notification:7' + "|{}".format(EMAIL_ADDR)
        data+="|No Long Running VMs"  # subject
        data+= "|There are no long running VMs." # body
        data = data.encode("utf-8") # Data must be a bytestring
        confirm_send = publisher.publish(topic_path, data) # Add two attributes, origin and username, to the message
        print(data)
        print(confirm_send.result())
        print(f"Published messages with custom attributes to {topic_path}.")
        
    return
	
long_running_vms(project='gcp-ent-auto-ds-dev', cloud_function=True)
