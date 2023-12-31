import base64
from google.cloud import bigquery
import logging
import modules
from modules import *
import json

logger = logging.getLogger()

def AssetLabeler(event, context):

    variables = get_variables_dynamic(event)
    client = bigquery.Client()

    for names in variables['ProjectName']:
        project = names
        date = variables['Date']
        #query = 'SELECT asset_type, update_time, requestTime, readTime, name as longname, resource.data as description FROM ti-dba-devenv-01.AssetInventory.assetinventory WHERE project = \'' + project + '\' and FORMAT_DATETIME(\'%F\',readTime) = \'' + date + '\''
        query = 'SELECT asset_type as type, update_time, requestTime, readTime, name as longname, resource.data as description, CASE LEFT(RIGHT(JSON_QUERY(resource.data, \'$.state\'),LENGTH(JSON_QUERY(resource.data, \'$.state\'))-1),LENGTH(JSON_QUERY(resource.data, \'$.state\'))-2) WHEN \'RUNNABLE\' THEN \'RUNNABLE\' WHEN \'RUNNING\' THEN \'RUNNING\' WHEN \'READY\' THEN \'READY\' ELSE \'NA\' END AS status, IFNULL(LEFT(RIGHT(JSON_QUERY(resource.data, \'$.settings.activationPolicy\'),LENGTH(JSON_QUERY(resource.data, \'$.settings.activationPolicy\'))-1),LENGTH(JSON_QUERY(resource.data, \'$.settings.activationPolicy\'))-2),\'NA\') AS activationPolicy, JSON_QUERY(resource.data, \'$.users\') AS users FROM ti-dba-devenv-01.AssetInventory.assetinventory WHERE FORMAT_DATETIME(\'%F\',readTime) = \'' + date + '\''
        #logger.warning(query)

        # Perform a query.
        QUERY = (
            query
            )
        query_job = client.query(QUERY)  # API request
        assets1 = query_job.result()  # Waits for query to finish

        for asset in assets1:
            if asset.type == 'compute.googleapis.com/Instance' and "compute.googleapis.com/Instance" in variables['ResourceType']:
                print("Working on a GCE")
                label_compute_instance(project,asset.longname,variables['LabelKey'],variables['LabelValue'])
                print("End GCE")
            elif asset.type == 'compute.googleapis.com/Disk' and "compute.googleapis.com/Disk" in variables['ResourceType'] and asset.users is not None:
                print("Working on a GCE DISK")
                label_compute_instance_disk(project,asset.longname,asset.users,variables['LabelKey'],variables['LabelValue'])
                print("End GCE DISK")
            elif asset.type == 'compute.googleapis.com/Disk' and "compute.googleapis.com/Disk" in variables['ResourceType'] and asset.users is None:
                print("Working on Orphan GCE DISK")
                label_compute_orphan_disk(project,asset.longname,"orphan",variables['LabelKey'],variables['LabelValue'])
                print("End Orphan GCE DISK")
            elif asset.type == 'sqladmin.googleapis.com/Instance' and asset.status == "RUNNABLE" and asset.activationPolicy == "ALWAYS" and "sqladmin.googleapis.com/Instance" in variables['ResourceType']:
                print("Working on a GCSQL")
                label_sqladmin_instance(project,asset.longname,variables['LabelKey'],variables['LabelValue'])
                print("End GCSQL")
            elif asset.type == 'storage.googleapis.com/Bucket' and "storage.googleapis.com/Bucket" in variables['ResourceType']:
                print("Working on a GCS")
                label_storage_bucket(asset.longname,variables['LabelKey'],variables['LabelValue'])
                print("End GCS")
            elif asset.type == 'bigquery.googleapis.com/Dataset' and "bigquery.googleapis.com/Dataset" in variables['ResourceType']:
                print("Working on a BQDS")
                label_bq_dataset(project,asset.longname,variables['LabelKey'],variables['LabelValue'])
                print("End BQDS")
