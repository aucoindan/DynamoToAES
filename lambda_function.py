'''
DDB_TO_ESS
Purpose of this script is to take data for a given DynamoDB Table and add it that into a elasticsearch index
It will add the data in an separate index name to add the info once that is complete it will delete the old index and alias the new index with the old index name
*** edit INDEX_VERSION arg if index has been re-indexed before *** 
References used
  - post_to_es_from_dynamodb.py taken from -> https://github.com/vladhoncharenko/aws-dynamodb-to-elasticsearch/blob/master/scripts/post_to_es_from_dynamodb.py
  - streaming function from -> https://github.com/aws-amplify/amplify-cli/blob/master/packages/graphql-elasticsearch-transformer/streaming-lambda/python_streaming_function.py
THINGS TO NOTE:
This script may need a few import statements need to run as mentioned below
FOLLOWING INFORMATION IS NEEDED
  - AWS REGION (must have same region as DynamoDB Table and Elasticsearch cluster)
  - TABLE NAME (full table name)
  - ELASTICSEARCH ENDPOINT (can be found on the aws console)
OPTIONAL
  - ACCESS AND SECRET If not provided it will grab credentials from the environment
'''

import json
import boto3
import boto3.dynamodb.types
from elasticsearch import Elasticsearch, RequestsHttpConnection, exceptions
from requests_aws4auth import AWS4Auth
import logging
import argparse
import urllib
from boto3 import Session
import base64
import datetime
import time
import traceback
from urllib.parse import urlparse, quote

# imports from this lambda function -> https://github.com/aws-amplify/amplify-cli/blob/master/packages/graphql-elasticsearch-transformer/streaming-lambda/python_streaming_function.py
from botocore.auth import SigV4Auth
from botocore.awsrequest import AWSRequest
from botocore.credentials import get_credentials
from botocore.httpsession import URLLib3Session
from botocore.session import Session as BotoSession
from boto3.dynamodb.types import TypeDeserializer

# edit this if on a new index version
# eg index name postv2 -> postv3
INDEX_VERSION = 1
# ElasticSearch 6 deprecated having multiple mapping types in an index. Default to doc.
DOC_TYPE = '_doc'
ES_MAX_RETRIES = 3 # Max number of retries for exponential backoff

# The following parameters are required to configure the ES cluster
ES_ENDPOINT = ''
ES_REGION = ''
DDB_TABLE_NAME = ''

logging.basicConfig()
logger = logging.getLogger()
logger.setLevel(logging.DEBUG)

region = ''
# not going to call lambda
# client = boto3.client('lambda', region_name='')
es = None
reports = []
object_amount = 0
partSize = 0
INDEX_NAME = ''
OLD_INDEX_NAME = ''


def main():
    global client
    global es_client
    global ES_ENDPOINT
    global ES_REGION
    global DDB_TABLE_NAME
    global INDEX_VERSION
    parser = argparse.ArgumentParser(description='Deletes table index and creates new index from ddb')
    parser.add_argument('--rn', metavar='R', help='AWS region', required=True)
    parser.add_argument('--tn', metavar='T', help='table name', required=True)
    parser.add_argument('--esd', metavar='ES_ENDPOINT', help='elasticsearch endpoint', required=True)
    parser.add_argument('--ak', metavar='AK', help='aws access key')
    parser.add_argument('--sk', metavar='AS', help='aws secret key')
    # using the lambda function locally do not need stream arn or lambda function arn
    # parser.add_argument('--esarn', metavar='ESARN', help='event source ARN', required=True)
    # parser.add_argument('--lf', metavar='LF', help='lambda function that posts data to es')
    args = parser.parse_args()

    scan_limit = 300
    args = parser.parse_args()
    
    # set global variables
    DDB_TABLE_NAME = args.tn
    ES_ENDPOINT = args.esd
    ES_REGION = args.rn

    if (args.ak is None or args.sk is None):
      credentials = boto3.Session().get_credentials()
      args.sk = args.sk or credentials.secret_key
      args.ak = args.ak or credentials.access_key
    
    # not going to call lambda since the code is mentioned below
    # client = boto3.client('lambda', region_name=args.rn)
    
    ## re-index
    create_new_index(args.tn, args.sk, args.ak, args.rn, args.esd)
    ## add records to ddb
    import_dynamodb_items_to_es(args.tn, args.sk, args.ak, args.rn, scan_limit) #, args.esarn, args.lf)
    ## delete old index and new index back
    re_index()

def create_new_index(table_name, aws_secret, aws_access, aws_region, es_domain):
  global INDEX_NAME
  global es
  global OLD_INDEX_NAME
  INDEX_NAME = "{}v{}".format(table_name.split('-')[0].lower(), INDEX_VERSION)
  OLD_INDEX_NAME = table_name.split('-')[0].lower()
  
  # Extract the domain name in ES_ENDPOINT
  es_url = urlparse(ES_ENDPOINT)
  es_endpoint = es_url.netloc or es_url.path
  awsauth = AWS4Auth(aws_access, aws_secret, ES_REGION, 'es')
  
  es = Elasticsearch(
      hosts = [{'host': es_endpoint, 'port': 443}],
      http_auth = awsauth,
      use_ssl = True,
      verify_certs = True,
      connection_class = RequestsHttpConnection
  )
  es.indices.create(index=INDEX_NAME)

def re_index():
  try:
    es.indices.delete(index=OLD_INDEX_NAME)
    # index is already deleted
  except exceptions.NotFoundError:
    pass
  es.indices.put_alias(index=INDEX_NAME, name=OLD_INDEX_NAME)

# removed arn and lambda function
def import_dynamodb_items_to_es(table_name, aws_secret, aws_access, aws_region, scan_limit):
    global reports
    global partSize
    global object_amount

    session = Session(aws_access_key_id=aws_access, aws_secret_access_key=aws_secret, region_name=aws_region)
    dynamodb = session.resource('dynamodb')
    logger.info('dynamodb: %s', dynamodb)
    ddb_table_name = table_name
    table = dynamodb.Table(ddb_table_name)
    logger.info('table: %s', table)
    ddb_keys_name = [a['AttributeName'] for a in table.attribute_definitions]
    logger.info('ddb_keys_name: %s', ddb_keys_name)
    response = None

    while True:
        if not response:
            response = table.scan(Limit=scan_limit)
        else:
            response = table.scan(ExclusiveStartKey=response['LastEvaluatedKey'], Limit=scan_limit)
        for i in response["Items"]:
            ddb_keys = {k: i[k] for k in i if k in ddb_keys_name}
            ddb_data = boto3.dynamodb.types.TypeSerializer().serialize(i)["M"]
            ddb_keys = boto3.dynamodb.types.TypeSerializer().serialize(ddb_keys)["M"]
            # PAYLOAD FROM DDB
            record = {
                "dynamodb": {"SequenceNumber": "0000", "Keys": ddb_keys, "NewImage": ddb_data},
                "awsRegion": aws_region,
                "eventName": "INSERT",
                "eventSourceARN": "",
                "eventSource": "aws:dynamodb"
            }
            partSize += 1
            object_amount += 1
            logger.info(object_amount)
            reports.append(record)

            if partSize >= 100:
                # send_to_eslambda(session, reports, lambda_f)
                send_to_eslambda(reports)

        if 'LastEvaluatedKey' not in response:
            break

    if partSize > 0:
        # send_to_eslambda(reports, lambda_f)
        send_to_eslambda(reports)


# def send_to_eslambda(session, items, lambda_f):
def send_to_eslambda(items):
    global reports
    global partSize
    records_data = {
        "Records": items
    }
    # records = json.dumps(records_data)
    # lambda_response = client.invoke(
    #     FunctionName=lambda_f,
    #     Payload=records
    # )
    lambda_response = lambda_handler(records_data, '')
    reports = []
    partSize = 0
    print(lambda_response)

'''
The following code is fromk AWS Amplify Python Streaming Function -> https://github.com/aws-amplify/amplify-cli/blob/master/packages/graphql-elasticsearch-transformer/streaming-lambda/python_streaming_function.py
'''

# custom encoder changes
# - sets to lists
class DDBTypesEncoder(json.JSONEncoder):
    def default(self, obj):
        if isinstance(obj, set):
            return list(obj)
        return json.JSONEncoder.default(self, obj)

# Subclass of boto's TypeDeserializer for DynamoDB to adjust for DynamoDB Stream format.
class StreamTypeDeserializer(TypeDeserializer):
    def _deserialize_n(self, value):
        return float(value)

    def _deserialize_b(self, value):
        return value  # Already in Base64


class ES_Exception(Exception):
    '''Capture status_code from request'''
    status_code = 0
    payload = ''

    def __init__(self, status_code, payload):
        self.status_code = status_code
        self.payload = payload
        Exception.__init__(
            self, 'ES_Exception: status_code={}, payload={}'.format(status_code, payload))


# Low-level POST data to Amazon Elasticsearch Service generating a Sigv4 signed request
def post_data_to_es(payload, region, creds, host, path, method='POST', proto='https://'):
    '''Post data to ES endpoint with SigV4 signed http headers'''
    req = AWSRequest(method=method, url=proto + host +
                    quote(path), data=payload, headers={'Host': host, 'Content-Type': 'application/json'})
    SigV4Auth(creds, 'es', region).add_auth(req)
    http_session = URLLib3Session()
    res = http_session.send(req.prepare())
    if res.status_code >= 200 and res.status_code <= 299:
        return res._content
    else:
        raise ES_Exception(res.status_code, res._content)


# High-level POST data to Amazon Elasticsearch Service with exponential backoff
# according to suggested algorithm: http://docs.aws.amazon.com/general/latest/gr/api-retries.html
def post_to_es(payload):
    '''Post data to ES cluster with exponential backoff'''

    # Get aws_region and credentials to post signed URL to ES
    es_region = ES_REGION or os.environ['AWS_REGION']
    session = BotoSession()
    creds = get_credentials(session)
    es_url = urlparse(ES_ENDPOINT)
    # Extract the domain name in ES_ENDPOINT
    es_endpoint = es_url.netloc or es_url.path

    # Post data with exponential backoff
    retries = 0
    while retries < ES_MAX_RETRIES:
        if retries > 0:
            seconds = (2 ** retries) * .1
            logger.debug('Waiting for %.1f seconds', seconds)
            time.sleep(seconds)

        try:
            es_ret_str = post_data_to_es(
                payload, es_region, creds, es_endpoint, '/_bulk')
            logger.debug('Return from ES: %s', es_ret_str)
            es_ret = json.loads(es_ret_str)

            if es_ret['errors']:
                logger.error(
                    'ES post unsuccessful, errors present, took=%sms', es_ret['took'])
                # Filter errors
                es_errors = [item for item in es_ret['items']
                            if item.get('index').get('error')]
                logger.error('List of items with errors: %s',
                            json.dumps(es_errors))
            else:
                logger.info('ES post successful, took=%sms', es_ret['took'])
            break  # Sending to ES was ok, break retry loop
        except ES_Exception as e:
            if (e.status_code >= 500) and (e.status_code <= 599):
                retries += 1  # Candidate for retry
            else:
                raise  # Stop retrying, re-raise exception


# Compute a compound doc index from the key(s) of the object in lexicographic order: "k1=key_val1|k2=key_val2"
def compute_doc_index(keys_raw, deserializer):
    index = []
    for key in sorted(keys_raw):
        index.append('{}={}'.format(
            key, deserializer.deserialize(keys_raw[key])))
    return '|'.join(index)


def _lambda_handler(event, context):
  logger.debug('Event: %s', event)
  records = event['Records']
  now = datetime.datetime.utcnow()

  ddb_deserializer = StreamTypeDeserializer()
  es_actions = []  # Items to be added/updated/removed from ES - for bulk API
  cnt_insert = cnt_modify = cnt_remove = 0

  for record in records:
      # Handle both native DynamoDB Streams or Streams data from Kinesis (for manual replay)
      logger.debug('Record: %s', record)
      if record.get('eventSource') == 'aws:dynamodb':
          ddb = record['dynamodb']
          ddb_table_name = DDB_TABLE_NAME
          doc_seq = ddb['SequenceNumber']
      elif record.get('eventSource') == 'aws:kinesis':
          ddb = json.loads(base64.b64decode(record['kinesis']['data']))
          ddb_table_name = ddb['SourceTable']
          doc_seq = record['kinesis']['sequenceNumber']
      else:
          logger.error('Ignoring non-DynamoDB event sources: %s',
                      record.get('eventSource'))
          continue

      # Compute DynamoDB table, type and index for item
      doc_table = ddb_table_name.lower()
      doc_type = DOC_TYPE
      doc_table_parts = doc_table.split('-')
      # override here to use index name
      doc_es_index_name = INDEX_NAME

      # Dispatch according to event TYPE
      event_name = record['eventName'].upper()  # INSERT, MODIFY, REMOVE
      logger.debug('doc_table=%s, event_name=%s, seq=%s',
                  doc_table, event_name, doc_seq)

      # Treat events from a Kinesis stream as INSERTs
      if event_name == 'AWS:KINESIS:RECORD':
          event_name = 'INSERT'

      # Update counters
      if event_name == 'INSERT':
          cnt_insert += 1
      elif event_name == 'MODIFY':
          cnt_modify += 1
      elif event_name == 'REMOVE':
          cnt_remove += 1
      else:
          logger.warning('Unsupported event_name: %s', event_name)

      is_ddb_insert_or_update = (event_name == 'INSERT') or (event_name == 'MODIFY')
      is_ddb_delete = event_name == 'REMOVE'
      image_name = 'NewImage' if is_ddb_insert_or_update else 'OldImage'

      if image_name not in ddb:
          logger.warning(
              'Cannot process stream if it does not contain ' + image_name)
          continue
      logger.debug(image_name + ': %s', ddb[image_name])
      # Deserialize DynamoDB type to Python types
      doc_fields = ddb_deserializer.deserialize({'M': ddb[image_name]})

      logger.debug('Deserialized doc_fields: %s', doc_fields)

      if ('Keys' in ddb):
          doc_id = compute_doc_index(ddb['Keys'], ddb_deserializer)
      else:
          logger.error('Cannot find keys in ddb record')

      # Generate JSON payload
      doc_json = json.dumps(doc_fields, cls=DDBTypesEncoder)

      # If DynamoDB INSERT or MODIFY, send 'index' to ES
      if is_ddb_insert_or_update:
          # Generate ES payload for item
          action = {'index': {'_index': doc_es_index_name,
                              '_type': doc_type, '_id': doc_id}}
          # Action line with 'index' directive
          es_actions.append(json.dumps(action))
          # Payload line
          es_actions.append(doc_json)

      # If DynamoDB REMOVE, send 'delete' to ES
      elif is_ddb_delete:
          action = {'delete': {'_index': doc_es_index_name,
                              '_type': doc_type, '_id': doc_id}}
          # Action line with 'index' directive
          es_actions.append(json.dumps(action))

  # Prepare bulk payload
  es_actions.append('')  # Add one empty line to force final \n
  es_payload = '\n'.join(es_actions)
  logger.info('Posting to ES: inserts=%s updates=%s deletes=%s, total_lines=%s, bytes_total=%s',
              cnt_insert, cnt_modify, cnt_remove, len(es_actions) - 1, len(es_payload))
  post_to_es(es_payload)  # Post to ES with exponential backoff


# Global lambda handler - catches all exceptions to avoid dead letter in the DynamoDB Stream
def lambda_handler(event, context):
  try:
    return _lambda_handler(event, context)
  except Exception:
      logger.error(traceback.format_exc())


'''
main execution point
'''

if __name__ == "__main__":
    main()
