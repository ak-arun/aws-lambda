import json
import boto3
import random
import cfnresponse
import string
import os
import urllib.request

ASSET_PREFIX="workshop/glue"
DQ_TBL_NAME = "seniorhealth_qa"
CRAWL_PREFIX = "health_qa/SeniorHealth_QA"
FAKE_DATA="_FakeData/"
XML_EXTN=".xml"

CFN_CREATE_STATUS=cfnresponse.SUCCESS
CFN_DELETE_STATUS=cfnresponse.SUCCESS

DQ_FAIL_PREFIX = "dqcheck/failed_records"
DQ_METRIX_PREFIX = "dqcheck/metrics"
DQ_PASS_PREFIX = "dqcheck/passed"

def create_bucket_on_no_exists(bucket_name):
  s3_client = boto3.client('s3')
  try:
    s3_client.head_bucket(Bucket=bucket_name)
    print("bucket {} already exists, not creating".format(bucket_name))
  except:
    s3_client.create_bucket(Bucket=bucket_name)
    print("bucket {} created".format(bucket_name))

def get_bucket_and_prefix(s3path):
  bkt = str((s3path.split("/")[2]))
  prefix = s3path.replace("s3://"+bkt+"/","")
  return(bkt,prefix)

def copy_file_from_url(src_url,target):
  tgtbp = get_bucket_and_prefix(target)
  target_bkt=tgtbp[0]
  target_key=tgtbp[1]
  try:
    response = urllib.request.urlopen(src_url)
    file_content = response.read()
    response.close()
    s3 = boto3.client('s3')
    s3.put_object(Body=file_content, Bucket=target_bkt, Key=target_key)
  except Exception as e:
    CFN_CREATE_STATUS=cfnresponse.FAILED
    print("Error occured while copying {} to {} and error is {}".format(src_url, target,e))

def copy_s3_folder(source_bucket, source_folder, target_bucket):
  response = None
  try:
      s3_client = boto3.client('s3')
      response = s3_client.list_objects_v2(Bucket=source_bucket, Prefix=source_folder)
      for obj in response.get('Contents', []):
          source_key = str(obj['Key'])
          source_url = "https://{}.s3.amazonaws.com/{}".format(source_bucket,source_key)
          parts = source_key.split('/')
          target_key = '/'.join(parts[1:])  
          target_path = "s3://{}/{}".format(target_bucket,target_key)
          copy_file_from_url(source_url,target_path)
  except Exception as e:
      CFN_CREATE_STATUS=cfnresponse.FAILED
      print("Exception {} occured and response is {} ".format(e,response))

def copy_fake_file(public_bkt, public_prefix,target_bucket):
    response = None
    try :
      s3_client = boto3.client('s3')
      response = s3_client.list_objects_v2(Bucket=public_bkt, Prefix=public_prefix)
      for obj in response.get('Contents', []):
        source_key = str(obj['Key'])
        source_url = "https://{}.s3.amazonaws.com/{}".format(public_bkt,source_key)
        filename = source_url.split("/")[-1]
        if XML_EXTN in filename:
          target_path = "s3://{}/{}".format(target_bucket,CRAWL_PREFIX+"/"+filename)
          copy_file_from_url(source_url,target_path)
    except Exception as e:
      CFN_CREATE_STATUS=cfnresponse.FAILED
      print("Exception {} occured while copying fake data set to s3 and response is {} ".format(e,response))


def create_database(db_name,db_location):
      try:
          glue = boto3.client('glue')
          glue.create_database(DatabaseInput={'Name': db_name,'LocationUri': db_location})
      except Exception as e:
          CFN_CREATE_STATUS=cfnresponse.FAILED
          print("Exection occured creating database {} and the exception is {}".format(db_name,e))

def create_and_start_crawler(crawler_name,glue_role_arn,db_name,crawl_location):
  client = boto3.client('glue')
  try:
      client.create_crawler(
          Name=crawler_name,
          Role=glue_role_arn,
          DatabaseName=db_name,
          Targets = {
              'S3Targets': [{
          'Path': crawl_location
              }]
          },
          SchemaChangePolicy = {
              'UpdateBehavior': 'UPDATE_IN_DATABASE',
              'DeleteBehavior': 'DEPRECATE_IN_DATABASE'
          }
          )
      print("Crawler {} created successfully".format(crawler_name))
      client.start_crawler(Name=crawler_name)
  except Exception as e:
      CFN_CREATE_STATUS=cfnresponse.FAILED
      print("Exception deploying crawler {} and the error is {}".format(crawler_name,e))

def get_common_job_params(glue_tmp_path_local,glue_spark_ui_log_path_local,spark_xml_lib_local):
  job_properties={}
  job_properties['--enable-glue-datacatalog'] = 'true'
  job_properties['--job-bookmark-option']= 'job-bookmark-disable'
  job_properties['--TempDir'] = glue_tmp_path_local
  job_properties['--enable-metrics'] = 'true'
  job_properties['--enable-spark-ui'] =  'true'
  job_properties['--spark-event-logs-path'] =  glue_spark_ui_log_path_local
  job_properties['--enable-job-insights'] =  'true'
  job_properties['--enable-continuous-cloudwatch-log']='true'
  job_properties['--job-language']= 'python'
  job_properties['--extra-jars']= spark_xml_lib_local
  return job_properties

def get_dq_job_params(dq_job_params,transformed_bucket,dq_db_name,dq_check_tblname):
  
  dq_job_params['--DQFAIL_RECORDS_PATH'] = "s3://{}/{}/".format(transformed_bucket,DQ_FAIL_PREFIX)
  dq_job_params['--DB_NAME'] = dq_db_name
  dq_job_params['--TBL_NAME'] = dq_check_tblname
  dq_job_params['--DQMETRICS_OUTPUT_PATH'] =  "s3://{}/{}/".format(transformed_bucket,DQ_METRIX_PREFIX)
  dq_job_params['--DQ_PASS_RECORDS_PATH'] =  "s3://{}/{}/".format(transformed_bucket,DQ_PASS_PREFIX)
  rules = """
  IsComplete "Answer",IsComplete "_pid"
  """
  dq_job_params['--RULES'] =  rules
  return dq_job_params

def deploy_glue_job(job_name,params,s3_script_path,job_role):
  glue = boto3.client('glue')
  try:
      glue.create_job(
              Name=job_name,
              GlueVersion="4.0",
              Role=job_role,
              Command={
                  'Name': 'glueetl',
                  'ScriptLocation': s3_script_path,
                  'PythonVersion': '3'
              },
              DefaultArguments = params,
              MaxRetries=0,
              NumberOfWorkers=2,
              WorkerType="G.1X"
          )
      print("Successfully deployed glue job {} ".format(job_name))
  except Exception as e:
      CFN_CREATE_STATUS=cfnresponse.FAILED
      print("Failed deployed glue job {} with error {} ".format(job_name,e))

def create_dq_fail_table(transformed_bucket,db_name,dq_failed_tbl):
  client = boto3.client('glue')
  try:
      sd = {
              "Columns": [{
                  "Name": "answer",
                  "Type": "string"
              }, {
                  "Name": "dataqualityevaluationresult",
                  "Type": "string"
              }, {
                  "Name": "dataqualityrulesskip",
                  "Type": "array<string>"
              }, {
                  "Name": "dataqualityrulesfail",
                  "Type": "array<string>"
              }, {
                  "Name": "dataqualityrulespass",
                  "Type": "array<string>"
              }, {
                  "Name": "question",
                  "Type": "struct<_VALUE:string,_qid:string,_qtype:string>"
              }, {
                  "Name": "_pid",
                  "Type": "int"
              }],
              "Location": "s3://{}/{}/".format(transformed_bucket,DQ_FAIL_PREFIX),
              "InputFormat": "org.apache.hadoop.mapred.TextInputFormat",
              "OutputFormat": "org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat",
              "Compressed": False,
              "SerdeInfo": {
                  "SerializationLibrary": "org.openx.data.jsonserde.JsonSerDe",
                  "Parameters": {
                      "paths": "Answer,DataQualityEvaluationResult,DataQualityRulesFail,DataQualityRulesPass,DataQualityRulesSkip,Question,_pid"
                  }
              },
              "BucketColumns": [],
              "SortColumns": [],
              "Parameters": {
                  "compressionType": "none",
                  "classification": "json",
                  "typeOfData": "file"
              },
              "StoredAsSubDirectories": False
          }

      response = client.create_table(
          DatabaseName =  db_name,
          TableInput={
              'Name': dq_failed_tbl,
              'StorageDescriptor': sd
          })
      print ("Successfully created glue table {} ".format(dq_failed_tbl))    
  except Exception as e:
      CFN_CREATE_STATUS=cfnresponse.FAILED
      print ("Failed to created glue table {} and error is ".format(dq_failed_tbl,e))  

def get_processing_job_params(params,transformed_bucket,qa_data_path):
    params["--INPUT_RECORDS_PATH"] = "s3://{}/{}/".format(transformed_bucket,DQ_PASS_PREFIX)
    params["--OUTPUT_RECORDS_PATH"] = "s3://{}/{}/".format(transformed_bucket,qa_data_path) 
    return params

def delete_database(glue,db_name):
  try:
    glue.delete_database(Name=db_name) 
    print("Glue DB {} deleted".format(db_name))
  except Exception as e:
    CFN_DELETE_STATUS=cfnresponse.FAILED
    print("Error deleting database {}. Error message is {} ".format(db_name,e))

def delete_crawler(glue,crawler_name):
  try:
    glue.delete_crawler(Name=crawler_name)
    print("Glue Crawler {} deleted".format(crawler_name))
  except Exception as e:
    CFN_DELETE_STATUS=cfnresponse.FAILED
    print("Error deleting crawler {}. Error message is {} ".format(crawler_name,e))

def delete_job(glue,job_name):
  try:
    glue.delete_job(JobName=job_name)
    print("Glue Job {} deleted".format(job_name))
  except Exception as e:
    CFN_DELETE_STATUS=cfnresponse.FAILED
    print("Error deleting job {}. Error message is {} ".format(job_name,e))

def delete_objects_in_bucket(bucket_name, prefix=None):
  try:
      s3_resource = boto3.resource('s3')
      s3_client = boto3.client('s3')
      bucket = s3_resource.Bucket(bucket_name)
      deleted_objects = []
      if prefix is None:
          for obj in bucket.objects.all():
              deleted_objects.append({'Key': obj.key})
      else:
          for obj in bucket.objects.filter(Prefix=prefix):
              deleted_objects.append({'Key': obj.key})
      if deleted_objects:
          response = s3_client.delete_objects(Bucket=bucket_name, Delete={'Objects': deleted_objects})
          print("Deleted Objects under bucket {} ".format(bucket_name))
  except Exception as e:
      CFN_DELETE_STATUS=cfnresponse.FAILED
      print("Error deleting {} with prefix {} . Error message is {} ".format(bucket_name,prefix,e))


def handle_delete(event, context):

  src_bucket=str(os.environ['src_bucket'])
  transformed_bucket=str(os.environ['transformed_bucket'])
  glue_dq_db_name=str(os.environ['glue_dq_db_name'])
  glue_crawler_name=str(os.environ['glue_crawler_name'])
  glue_processing_job_name=str(os.environ['glue_processing_job_name'])
  glue_dq_job_name=str(os.environ['glue_dq_job_name'])
  account_id=str(os.environ['account_id'])
  aws_region=str(os.environ['aws_region'])
  glue_assets_bucket_name = "aws-glue-assets-{}-{}".format(account_id,aws_region)

  delete_objects_in_bucket(glue_assets_bucket_name,ASSET_PREFIX)
  delete_objects_in_bucket(src_bucket,None)
  delete_objects_in_bucket(transformed_bucket,None)
  glue = boto3.client('glue')
  delete_database(glue,glue_dq_db_name)
  #delete_crawler(glue,glue_crawler_name)
  #delete_job(glue,glue_dq_job_name)
  #delete_job(glue,glue_processing_job_name)
  physical_resourceId = ''.join(random.choices(string.ascii_lowercase +string.digits, k=7))
  cfnresponse.send(event,context,CFN_DELETE_STATUS,{},physical_resourceId) 

def handle_create(event,context):
  
  src_bucket=str(os.environ['src_bucket'])
  transformed_bucket=str(os.environ['transformed_bucket'])
  account_id=str(os.environ['account_id'])
  aws_region=str(os.environ['aws_region'])
  bucket_prefix=str(os.environ['bucket_prefix'])
  qa_data_path=str(os.environ['qa_data_path'])
  glue_dq_script=str(os.environ['glue_dq_script'])
  glue_processqa_script=str(os.environ['glue_processqa_script'])
  xml_lib_path=str(os.environ['xml_lib_path'])
  glue_dq_db_name=str(os.environ['glue_dq_db_name'])
  glue_crawler_name=str(os.environ['glue_crawler_name'])
  glue_dq_job_name=str(os.environ['glue_dq_job_name'])
  glue_processing_job_name=str(os.environ['glue_processing_job_name'])
  glue_role_arn=str(os.environ['glue_role_arn'])
  glue_dq_fail_tbl_name = str(os.environ['glue_dq_fail_tbl_name'])
  glue_processed_data_path_prefix = str(os.environ['glue_processed_data_path_prefix'])

  glue_assets_bucket_name = "aws-glue-assets-{}-{}".format(account_id,aws_region)
  create_bucket_on_no_exists(glue_assets_bucket_name)

  dq_script_local = "s3://{}/{}/scripts/{}.py".format(glue_assets_bucket_name,ASSET_PREFIX,glue_dq_job_name)
  processing_script_local = "s3://{}/{}/scripts/{}.py".format(glue_assets_bucket_name,ASSET_PREFIX,glue_processing_job_name)
  spark_xml_lib_local = "s3://{}/{}/libs/spark-xml_2.12-0.16.0.jar".format(glue_assets_bucket_name,ASSET_PREFIX)
  glue_tmp_path_local = "s3://{}/{}/tmp/".format(glue_assets_bucket_name,ASSET_PREFIX)
  glue_spark_ui_log_path_local = "s3://{}/{}/sparkuilogs/".format(glue_assets_bucket_name,ASSET_PREFIX)
  glue_db_path_local = "s3://{}/{}/databases/{}/".format(glue_assets_bucket_name,ASSET_PREFIX,glue_dq_db_name)
  glue_crawl_path = "s3://{}/{}/".format(src_bucket,CRAWL_PREFIX)
  
  #copy dq glue job script from public to local bucket
  copy_file_from_url(glue_dq_script,dq_script_local)
  
  #copy Q&A data processing glue job script from public to local bucket
  copy_file_from_url(glue_processqa_script,processing_script_local)
  
  #copy XML jar from public to local bucket
  copy_file_from_url(xml_lib_path,spark_xml_lib_local)

  # copy the data from public bucket to source bucket
  data_public_b_and_p = get_bucket_and_prefix(qa_data_path)
  copy_s3_folder(data_public_b_and_p[0], data_public_b_and_p[1], src_bucket)

  # DQ check is done on a simulated data. Copy the simulated data to the table directory
  copy_fake_file(data_public_b_and_p[0],data_public_b_and_p[1][:-1]+FAKE_DATA,src_bucket)

  create_database(glue_dq_db_name,glue_db_path_local)
  #create_and_start_crawler(glue_crawler_name,glue_role_arn,glue_dq_db_name,glue_crawl_path)

  job_properties=get_common_job_params(glue_tmp_path_local,glue_spark_ui_log_path_local,spark_xml_lib_local)
  
  glue_dq_job_prop = get_dq_job_params(job_properties,transformed_bucket,glue_dq_db_name,DQ_TBL_NAME)
  #deploy_glue_job(glue_dq_job_name,glue_dq_job_prop,dq_script_local,glue_role_arn)

  create_dq_fail_table(transformed_bucket,glue_dq_db_name,glue_dq_fail_tbl_name)

  job_properties=get_common_job_params(glue_tmp_path_local,glue_spark_ui_log_path_local,spark_xml_lib_local)
  glue_qaprocessing_job_prop = get_processing_job_params(job_properties,transformed_bucket,glue_processed_data_path_prefix)
  #deploy_glue_job(glue_processing_job_name,glue_qaprocessing_job_prop,processing_script_local,glue_role_arn)

  physical_resourceId = ''.join(random.choices(string.ascii_lowercase +string.digits, k=7))
  cfnresponse.send(event,context,CFN_CREATE_STATUS,{},physical_resourceId) 

def lambda_handler(event, context):
    return_dict ={}
    request_type = event.get("RequestType","")
    if request_type=='Create':
      handle_create(event,context)
    elif request_type =='Delete':
      handle_delete(event, context)
    else:
      return return_dict
