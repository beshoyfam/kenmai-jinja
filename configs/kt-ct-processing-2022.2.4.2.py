"""
Airflow DAG for dayly CT processing.
CT version compatibility: 2022.2.4.x

TESTING DAG for both vendors, encrypted input and 1 full day processing
This DAG will create 2 clusters and process each vendor.

We use 2 process date variables, one for the EDR processing day (VAR_PROCESS_DATE_EDR)
and another one for the ween processing, raster and ADELE (VAR_PROCESS_DATE)

"""

import datetime
import os

from airflow import models
from airflow.contrib.operators import dataproc_operator
from airflow.contrib.operators import gcs_to_bq, bigquery_operator
from airflow.operators.python_operator import BranchPythonOperator
from airflow.operators.dummy_operator import DummyOperator
import six.moves.urllib.parse
from airflow.utils import trigger_rule
from airflow.models import Variable

##################################################
#### Constants / Variables
##################################################
dag_config = Variable.get("kt-ct-processing-2022.2.4.2-vars", deserialize_json=True)

# Common variables
VAR_CLUSTER = dag_config["VAR_CLUSTER"]
VAR_GCE_REGION = dag_config["VAR_GCE_REGION"]
VAR_GCP_PROJECT = dag_config["VAR_GCP_PROJECT"]
DATAPROC_IMAGE = dag_config["DATAPROC_IMAGE"]

SERV_ACCOUNT = dag_config["SERV_ACCOUNT"]
SUBNET_URI = 'https://www.googleapis.com/compute/v1/projects/' + \
    VAR_GCP_PROJECT + '/regions/' + VAR_GCE_REGION + '/subnetworks/{{SUBNET_URI}}'

INIT_ACTIONS_SCRIPT = dag_config["INIT_ACTIONS_SCRIPT"]
VAR_NETPERFORM_OUTPUT_PATH = dag_config["VAR_NETPERFORM_OUTPUT_PATH"]
LOG_CONFIG_FILE_PATH = dag_config["LOG_CONFIG_FILE_PATH"]
PROCESSOR_JAR = dag_config["PROCESSOR_JAR"]
PROCESSOR_CLASS = dag_config["PROCESSOR_CLASS"]
RASTER_JAR = dag_config["RASTER_JAR"]
RASTER_CLASS = dag_config["RASTER_CLASS"]
ADELE_JAR = dag_config["ADELE_JAR"]
ADELE_CLASS = dag_config["ADELE_CLASS"]
RESHUFFLING_JAR = dag_config["RESHUFFLING_JAR"]
RESHUFFLING_CLASS = dag_config["RESHUFFLING_CLASS"]
VAR_CT_LIB1 = dag_config["VAR_CT_LIB1"]
VAR_CT_LIB2 = dag_config["VAR_CT_LIB2"]
VAR_CT_LIB3 = dag_config["VAR_CT_LIB3"]
VAR_CT_LIB4 = dag_config["VAR_CT_LIB4"]
VAR_CT_LIB5 = dag_config["VAR_CT_LIB5"]
VAR_CT_LIB6 = dag_config["VAR_CT_LIB6"]
VAR_CT_LIB7 = dag_config["VAR_CT_LIB7"]

VAR_PROCESS_DATE = dag_config["VAR_PROCESS_DATE"]
VAR_PROCESS_DATE_EDR = dag_config["VAR_PROCESS_DATE_EDR"]

# Ericsson variables
VAR_ERI_CLUSTER = VAR_CLUSTER + '-eri'
CT_CONFIG_PROCESSOR_ERI_ENC = dag_config["CT_CONFIG_PROCESSOR_ERI_ENC"]
CT_CONFIG_RASTER_ERI_ENC = dag_config["CT_CONFIG_RASTER_ERI_ENC"]
CT_CONFIG_ADELE_ERI_ENC = dag_config["CT_CONFIG_ADELE_ERI_ENC"]
CT_CONFIG_RESHUFFLING_ERI_ENC_INPUT_PATH = dag_config["CT_CONFIG_RESHUFFLING_ERI_ENC_INPUT_PATH"]
CT_CONFIG_RESHUFFLING_ERI_ENC_OUTPUT_PATH = dag_config["CT_CONFIG_RESHUFFLING_ERI_ENC_OUTPUT_PATH"]

# Huawei variables
VAR_HUA_CLUSTER = VAR_CLUSTER + '-hua'
CT_CONFIG_PROCESSOR_HUA_ENC = dag_config["CT_CONFIG_PROCESSOR_HUA_ENC"]
CT_CONFIG_RASTER_HUA_ENC = dag_config["CT_CONFIG_RASTER_HUA_ENC"]
CT_CONFIG_ADELE_HUA_ENC = dag_config["CT_CONFIG_ADELE_HUA_ENC"]
CT_CONFIG_RESHUFFLING_HUA_ENC_INPUT_PATH = dag_config["CT_CONFIG_RESHUFFLING_HUA_ENC_INPUT_PATH"]
CT_CONFIG_RESHUFFLING_HUA_ENC_OUTPUT_PATH = dag_config["CT_CONFIG_RESHUFFLING_HUA_ENC_OUTPUT_PATH"]

# Calutate epoch time start and end.
# In automatic mode, we will only need one date

start_epoch = int((datetime.datetime.strptime(VAR_PROCESS_DATE_EDR + "/00:00:00", "%Y-%m-%d/%H:%M:%S")).timestamp())
end_epoch = int((datetime.datetime.strptime(VAR_PROCESS_DATE_EDR + "/23:59:59", "%Y-%m-%d/%H:%M:%S")).timestamp())

yesterday = datetime.datetime.combine(
    datetime.datetime.today() - datetime.timedelta(1),
    datetime.datetime.min.time())


default_dag_args = {
    # Setting start date as yesterday starts the DAG immediately when it is
    # detected in the Cloud Storage bucket.
    'start_date': yesterday,
    # To email on failure or retry set 'email' arg to your email and enable
    # emailing here.
    'email_on_failure': False,
    'email_on_retry': False,
    # If a task fails, retry it once after waiting at least 5 minutes
    'retries': 1,
    'retry_delay': datetime.timedelta(minutes=5),
    'project_id': VAR_GCP_PROJECT
}

cluster_properties = {
    "core:fs.gs.status.parallel.enable": "true",
    "core:fs.gs.implicit.dir.repair.enable": "false"}

##################################################
# Spark properties
##################################################

spark_custom_properties = {
    "spark.port.maxRetries": "100",
    "spark.executor.memoryOverhead": "6g",
    "spark.default.parallelism": "80",
    "geospark.global.charset": "utf8",
    "spark.executor.instances": "20",
    "spark.executor.memory": "20g",
    "spark.executor.cores": "4",
    "spark.driver.memoryOverhead": "2g",
    "spark.driver.memory": "12g",
    "spark.sql.autoBroadcastJoinThreshold": "0",
    "spark.eventLog.logBlockUpdates.enabled": "true",
    "spark.io.compression.lz4.blockSize": "512Kb",
    "spark.shuffle.service.index.cache.size": "2048",
    "spark.file.transferTo": "false",
    "spark.shuffle.file.buffer": "1MB",
    "spark.shuffle.unsafe.file.output.buffer": "5MB",
    "spark.unsafe.sorter.spill.reader.buffer.size": "1MB",
    "spark.dynamicAllocation.enabled": "false",
    "spark.executor.extraJavaOptions": "-XX:MaxDirectMemorySize=8000m \
    -Dgeospark.global.charset=utf8 \
    -XX:+UseCompressedOops \
    -XX:ParallelGCThreads=4 \
    -XX:+UseParallelGC \
    -Dorg.geotools.referencing.forceXY=true \
    -Dlog4j.configuration=file://adele_log4j.properties"}


spark_reshuffle_properties = {
    "spark.port.maxRetries": "100",
    "spark.executor.memoryOverhead": "6g",
    "spark.default.parallelism": "80",
    "geospark.global.charset": "utf8",
    "spark.executor.instances": "20",
    "spark.executor.memory": "20g",
    "spark.executor.cores": "4",
    "spark.driver.memoryOverhead": "2g",
    "spark.driver.memory": "12g",
    "spark.executor.userClassPathFirst": "true",
    "spark.driver.userClassPathFirst": "true",
    "spark.sql.autoBroadcastJoinThreshold": "0",
    "spark.eventLog.logBlockUpdates.enabled": "true",
    "spark.io.compression.lz4.blockSize": "512Kb",
    "spark.shuffle.service.index.cache.size": "2048",
    "spark.file.transferTo": "false",
    "spark.shuffle.file.buffer": "1MB",
    "spark.shuffle.unsafe.file.output.buffer": "5MB",
    "spark.unsafe.sorter.spill.reader.buffer.size": "1MB",
    "spark.dynamicAllocation.enabled": "false",
    "spark.executor.extraJavaOptions": "-XX:MaxDirectMemorySize=8000m \
    -Dgeospark.global.charset=utf8 \
    -XX:+UseCompressedOops \
    -XX:ParallelGCThreads=4 \
    -XX:+UseParallelGC \
    -Dorg.geotools.referencing.forceXY=true \
    -Dlog4j.configuration=file://adele_log4j.properties"}


class CustomDataprocClusterCreateOperator(dataproc_operator.DataprocClusterCreateOperator):

    def __init__(self, *args, **kwargs):
        super(CustomDataprocClusterCreateOperator, self).__init__(*args, **kwargs)

    def _build_cluster_data(self):
        cluster_data = super(CustomDataprocClusterCreateOperator, self)._build_cluster_data()
        cluster_data['config']['endpointConfig'] = {
            'enableHttpPortAccess': True
        }
        # cluster_data['config']['softwareConfig']['optionalComponents'] = [ 'JUPYTER', 'ANACONDA' ]
        return cluster_data


##################################################
# DAG
##################################################
with models.DAG(
        'kt-ct-processing-2022.2.4.2',
        schedule_interval=None,
        default_args=default_dag_args) as dag:

    # Create a Cloud Dataproc cluster.
    # create_dataproc_cluster = dataproc_operator.DataprocClusterCreateOperator(
    create_dataproc_cluster_eri = CustomDataprocClusterCreateOperator(
        task_id='create_dataproc_cluster_eri',
        cluster_name=VAR_ERI_CLUSTER,
        service_account=SERV_ACCOUNT,
        num_workers=5,
        region=VAR_GCE_REGION,
        internal_ip_only=True,
        idle_delete_ttl=14400,
        tags=['allow-internal-dataproc-proda',
              'allow-ssh-from-management-zone', 'allow-sshfrom-net-to-bastion'],
        subnetwork_uri=SUBNET_URI,
        properties=cluster_properties,
        master_disk_size=150,
        worker_disk_size=1100,
        init_actions_uris=[INIT_ACTIONS_SCRIPT],
        image_version=DATAPROC_IMAGE,
        master_machine_type='n2-standard-8',
        worker_machine_type='e2-highmem-16')

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster_eri = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster_eri',
        # zone=VAR_GCE_ZONE,
        region=VAR_GCE_REGION,
        cluster_name=VAR_ERI_CLUSTER,
        service_account=SERV_ACCOUNT,
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    create_dataproc_cluster_hua = CustomDataprocClusterCreateOperator(
        task_id='create_dataproc_cluster_hua',
        cluster_name=VAR_HUA_CLUSTER,
        service_account=SERV_ACCOUNT,
        num_workers=5,
        region=VAR_GCE_REGION,
        internal_ip_only=True,
        idle_delete_ttl=14400,
        tags=['allow-internal-dataproc-proda',
              'allow-ssh-from-management-zone', 'allow-sshfrom-net-to-bastion'],
        subnetwork_uri=SUBNET_URI,
        properties=cluster_properties,
        master_disk_size=150,
        worker_disk_size=1100,
        init_actions_uris=[INIT_ACTIONS_SCRIPT],
        image_version=DATAPROC_IMAGE,
        master_machine_type='n2-standard-8',
        worker_machine_type='e2-highmem-16')

    # Delete Cloud Dataproc cluster.
    delete_dataproc_cluster_hua = dataproc_operator.DataprocClusterDeleteOperator(
        task_id='delete_dataproc_cluster_hua',
        # zone=VAR_GCE_ZONE,
        region=VAR_GCE_REGION,
        cluster_name=VAR_HUA_CLUSTER,
        service_account=SERV_ACCOUNT,
        # Setting trigger_rule to ALL_DONE causes the cluster to be deleted
        # even if the Dataproc job fails.
        trigger_rule=trigger_rule.TriggerRule.ALL_DONE)

    launch_eri_enc_reshuffling_job = dataproc_operator.DataProcSparkOperator(
        task_id='launch_eri_enc_reshuffling_job',
        cluster_name=VAR_ERI_CLUSTER,
        region=VAR_GCE_REGION,
        files=[LOG_CONFIG_FILE_PATH, VAR_CT_LIB1,
                VAR_CT_LIB2, VAR_CT_LIB3, VAR_CT_LIB4, VAR_CT_LIB5,
                VAR_CT_LIB6, VAR_CT_LIB7],
        dataproc_spark_properties=spark_reshuffle_properties,
        arguments=[CT_CONFIG_RESHUFFLING_ERI_ENC_INPUT_PATH,
                   CT_CONFIG_RESHUFFLING_ERI_ENC_OUTPUT_PATH, str(start_epoch), str(end_epoch), 'ericsson'],
        dataproc_spark_jars=[RESHUFFLING_JAR],
        main_class=RESHUFFLING_CLASS,
    )

    launch_hua_enc_reshuffling_job = dataproc_operator.DataProcSparkOperator(
        task_id='launch_hua_enc_reshuffling_job',
        cluster_name=VAR_HUA_CLUSTER,
        region=VAR_GCE_REGION,
        files=[LOG_CONFIG_FILE_PATH, VAR_CT_LIB1,
                VAR_CT_LIB2, VAR_CT_LIB3, VAR_CT_LIB4, VAR_CT_LIB5,
                VAR_CT_LIB6, VAR_CT_LIB7],
        dataproc_spark_properties=spark_reshuffle_properties,
        arguments=[CT_CONFIG_RESHUFFLING_HUA_ENC_INPUT_PATH,
                   CT_CONFIG_RESHUFFLING_HUA_ENC_OUTPUT_PATH, str(start_epoch), str(end_epoch), 'huawei'],
        dataproc_spark_jars=[RESHUFFLING_JAR],
        main_class=RESHUFFLING_CLASS,
    )

    launch_eri_enc_processor_job = dataproc_operator.DataProcSparkOperator(
        task_id='launch_eri_enc_processor_job',
        cluster_name=VAR_ERI_CLUSTER,
        region=VAR_GCE_REGION,
        files=[CT_CONFIG_PROCESSOR_ERI_ENC, LOG_CONFIG_FILE_PATH, VAR_CT_LIB1,
                VAR_CT_LIB2, VAR_CT_LIB3, VAR_CT_LIB4, VAR_CT_LIB5,
                VAR_CT_LIB6, VAR_CT_LIB7],
        dataproc_spark_properties=spark_custom_properties,
        arguments=[CT_CONFIG_PROCESSOR_ERI_ENC.rsplit(
            '/', 1)[-1], VAR_PROCESS_DATE_EDR, '*', '-h', VAR_NETPERFORM_OUTPUT_PATH],
        dataproc_spark_jars=[PROCESSOR_JAR],
        main_class=PROCESSOR_CLASS,
    )

    launch_hua_enc_processor_job = dataproc_operator.DataProcSparkOperator(
        task_id='launch_hua_enc_processor_job',
        cluster_name=VAR_HUA_CLUSTER,
        region=VAR_GCE_REGION,
        files=[CT_CONFIG_PROCESSOR_HUA_ENC, LOG_CONFIG_FILE_PATH, VAR_CT_LIB1,
                VAR_CT_LIB2, VAR_CT_LIB3, VAR_CT_LIB4, VAR_CT_LIB5,
                VAR_CT_LIB6, VAR_CT_LIB7],
        dataproc_spark_properties=spark_custom_properties,
        arguments=[CT_CONFIG_PROCESSOR_HUA_ENC.rsplit(
            '/', 1)[-1], VAR_PROCESS_DATE_EDR, '*', '-h', VAR_NETPERFORM_OUTPUT_PATH],
        dataproc_spark_jars=[PROCESSOR_JAR],
        main_class=PROCESSOR_CLASS,
    )

    launch_eri_enc_raster_job = dataproc_operator.DataProcSparkOperator(
        task_id='launch_eri_enc_raster_job',
        cluster_name=VAR_ERI_CLUSTER,
        region=VAR_GCE_REGION,
        files=[CT_CONFIG_RASTER_ERI_ENC, LOG_CONFIG_FILE_PATH],
        dataproc_spark_properties=spark_custom_properties,
        arguments=[CT_CONFIG_RASTER_ERI_ENC.rsplit(
            '/', 1)[-1], '100-1w', VAR_PROCESS_DATE],
        dataproc_spark_jars=[RASTER_JAR],
        main_class=RASTER_CLASS,
    )

    launch_hua_enc_raster_job = dataproc_operator.DataProcSparkOperator(
        task_id='launch_hua_enc_raster_job',
        cluster_name=VAR_HUA_CLUSTER,
        region=VAR_GCE_REGION,
        files=[CT_CONFIG_RASTER_HUA_ENC, LOG_CONFIG_FILE_PATH],
        dataproc_spark_properties=spark_custom_properties,
        arguments=[CT_CONFIG_RASTER_HUA_ENC.rsplit(
            '/', 1)[-1], '100-1w', VAR_PROCESS_DATE],
        dataproc_spark_jars=[RASTER_JAR],
        main_class=RASTER_CLASS,
    )

    launch_eri_enc_adele_job = dataproc_operator.DataProcSparkOperator(
        task_id='launch_eri_enc_adele_job',
        cluster_name=VAR_ERI_CLUSTER,
        region=VAR_GCE_REGION,
        files=[CT_CONFIG_ADELE_ERI_ENC, LOG_CONFIG_FILE_PATH],
        dataproc_spark_properties=spark_custom_properties,
        arguments=[CT_CONFIG_ADELE_ERI_ENC.rsplit(
            '/', 1)[-1], 'calltraces', VAR_PROCESS_DATE],
        dataproc_spark_jars=[ADELE_JAR],
        main_class=ADELE_CLASS,
    )

    launch_hua_enc_adele_job = dataproc_operator.DataProcSparkOperator(
        task_id='launch_hua_enc_adele_job',
        cluster_name=VAR_HUA_CLUSTER,
        region=VAR_GCE_REGION,
        files=[CT_CONFIG_ADELE_HUA_ENC, LOG_CONFIG_FILE_PATH],
        dataproc_spark_properties=spark_custom_properties,
        arguments=[CT_CONFIG_ADELE_HUA_ENC.rsplit(
            '/', 1)[-1], 'calltraces', VAR_PROCESS_DATE],
        dataproc_spark_jars=[ADELE_JAR],
        main_class=ADELE_CLASS,
    )

    # Define DAG dependencies.
    create_dataproc_cluster_eri >> launch_eri_enc_reshuffling_job >> launch_eri_enc_processor_job >> launch_eri_enc_raster_job >> launch_eri_enc_adele_job >> delete_dataproc_cluster_eri
    create_dataproc_cluster_hua >> launch_hua_enc_reshuffling_job >> launch_hua_enc_processor_job >> launch_hua_enc_raster_job >> launch_hua_enc_adele_job >> delete_dataproc_cluster_hua
