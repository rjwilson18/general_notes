from airflow import DAG
from airflow.operators.bash_operator import BashOperator
from airflow.utils.dates import days_ago
from airflow import models
from airflow.kubernetes.secret import Secret
from airflow.contrib.operators.kubernetes_pod_operator import KubernetesPodOperator
from airflow.models import Variable
import os

#define constants
DAG_NAME = 'pgp_encryption'
TASK_ID = 'pgp_encrypt'
PROJECT_ID = 'project_id'
TIMEOUT_PERIOD = 360 #seconds
IMAGE_VERSION = 'latest'
DOCKER_IMAGE = 'gcr.io/{0}/pgp_encryption:{1}'.format(PROJECT_ID,IMAGE_VERSION)
MAX_CONCURRENT_RUNS = 100


with DAG(
    dag_id=DAG_NAME,
    concurrency = MAX_CONCURRENT_RUNS,
    max_active_runs = MAX_CONCURRENT_RUNS,    
    start_date=days_ago(1),
    schedule_interval=None) as dag:


    
    # Print the received dag_run configuration.
    # The DAG run configuration contains information about the
    # Cloud Storage object change.
    t1 = KubernetesPodOperator(
    task_id=TASK_ID,
    name=TASK_ID,
    namespace='default',
    resources={'request_memory': '1Gi',
                   'request_cpu': '500m',
                   'limit_memory': '10Gi',
                   'limit_cpu': 1},
    startup_timeout_seconds=TIMEOUT_PERIOD,
    image=DOCKER_IMAGE,
    image_pull_policy='Always',
    env_vars={'input_file': '{{ dag_run.conf["input_file"] }}'},
    is_delete_operator_pod=True,
    affinity={
        'nodeAffinity': {
            # requiredDuringSchedulingIgnoredDuringExecution means in order
            # for a pod to be scheduled on a node, the node must have the
            # specified labels. However, if labels on a node change at
            # runtime such that the affinity rules on a pod are no longer
            # met, the pod will still continue to run on the node.
            'requiredDuringSchedulingIgnoredDuringExecution': {
                'nodeSelectorTerms': [{
                    'matchExpressions': [{
                        # When nodepools are created in Google Kubernetes
                        # Engine, the nodes inside of that nodepool are
                        # automatically assigned the label
                        # 'cloud.google.com/gke-nodepool' with the value of
                        # the nodepool's name.
                        'key': 'cloud.google.com/gke-nodepool',
                        'operator': 'In',
                        # The label key's value that pods can be scheduled
                        # on.
                        'values': [
                            'container-run-pool'
                        ]
                    }]
                }]
            }
        }
    })


    t1
