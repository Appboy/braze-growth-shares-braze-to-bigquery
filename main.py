from flask import Flask, render_template, request, Response, abort, jsonify
from google.cloud import bigquery, storage

import collections
import os

from datetime import datetime, timedelta
import re
import gzip
import json
import requests
import csv
import urllib3
from requests.adapters import HTTPAdapter
from requests.packages.urllib3.util.retry import Retry
import io
import zipfile
import logging
import boto3


# If `entrypoint` is not defined in app.yaml, App Engine will look for an app
# called `app` in `main.py`.
app = Flask(__name__)

# Endpoint to queue Braze Segment Export to be run via gcs cron job.
@app.route('/schedule', methods=['POST','GET'])
def queueBrazeSegmentExport():
    # Insure origin source is from google cron job
    if (request.headers.get('X-Appengine-Cron') is None ) and request.headers.get('X-AppEngine-QueueName') is None:
        logging.critical("Schedule from outside Google App Engine")
        abort(401)
    brazeurl = os.environ['brazerestendpoint'].rstrip('/')  + os.environ['brazesegmentendpoint']
    brazeapikey = os.environ['brazeapikey'].strip()
    brazesegmentid = os.environ['brazesegmentid'].strip()
    brazefields = [sf.strip() for sf in os.environ['brazesegmentfields'].split(',')]
    if os.environ['gcsplatformprefix'] is None:
        gcsplatformprefix = '.appspot.com'
    else:
        gcsplatformprefix = os.environ['gcsplatformprefix'].strip().lower()

    # Generate call back url
    callback_url = "https://{}{}/{}".format(os.environ['GOOGLE_CLOUD_PROJECT'], gcsplatformprefix, 'exportcallback' )
    brazepayload = {
        "api_key": brazeapikey,
        "segment_id": brazesegmentid,
        "fields_to_export": brazefields,
        "callback_endpoint": callback_url,
        "output_format": "zip"
    }
    brazeresponse = requests.post(brazeurl, json = brazepayload)
    if (200 <= brazeresponse.status_code  < 300):
        result = brazeresponse.json()
        if 'message' in result:
            if result['message'] == 'success':
                resp = Response('{"message": "ok"}', status=200, mimetype='application/json')
            else:
                logging.error("Segment Export Failure: {}".format(json.dumps(result)))
                resp = Response('{"message": "error"}', status=200, mimetype='application/json')
            return resp
        else:
            logging.error("Segment Export Failure: {}".format(json.dumps(result)))
            resp = Response('{"message": "error"}', status=200, mimetype='application/json')
            return resp
    else:
        logging.error("Segment Export Failure Response")
        resp = Response('{"message": "error"}', status=200, mimetype='application/json')
        return resp

# Method to process s3 url, and save to cloud storage
def processURLGCS(url):
    brazeexport = requests.get(url)
    brazefields = [sf.strip() for sf in os.environ['brazesegmentfields'].split(',')]

    # Create Google Cloud Storage Bucket file to save file to save to
    storage_client = storage.Client()
    gcsbucketname = os.environ['gcsproject'].lower().strip()
    if os.environ['gcsplatformprefix'] is None:
        gcsbucketname += '.appspot.com'
    else:
        gcsbucketname += os.environ['gcsplatformprefix'].strip().lower()

    gcsbucket = storage_client.get_bucket(gcsbucketname)
    gcsfilename = ''
    if (os.environ['gcspath']):
        gcsfilename += os.environ['gcspath'].strip() + '/'
    gcsbasefilename = "braze_export_{}".format(datetime.utcnow().strftime("%s"))
    gcsfilename += gcsbasefilename + '.csv'
    gcsfile = gcsbucket.blob(gcsfilename)
    content_type = 'text/csv'

    gcscsv = io.StringIO()
    csvwriter = csv.writer(gcscsv, delimiter=',')
    gcscsv.write(','.join(brazefields) + "\n")
    with zipfile.ZipFile(io.BytesIO(brazeexport.content)) as exports:
        for brazefiles in exports.infolist():
            if '.txt' in brazefiles.filename:
                with exports.open(brazefiles, 'r') as brazefile:
                    linecount = 0
                    for line in brazefile:
                        linecount += 1
                        try:
                            line_utf8 = line.decode("utf-8").rstrip()
                            record_json = json.loads(line_utf8)
                            record = []
                            for sfv in brazefields:
                                if sfv in record_json:
                                    record.append(record_json[sfv])
                                else:
                                    record.append(None)
                            csvwriter.writerow(record)
                            logging.info("Record {}".format(' '.join(str(x) for x in record)))
                        except Exception as e :
                            logging.warning("Parse error in {} line {}: {}\n\tError: {}"
                                .format(brazefiles.filename, linecount, line_utf8, e))
                            continue
    try:
        # Write file to cloud storage
        gcsfile.upload_from_string(
            gcscsv.getvalue(),
            content_type = content_type,
            predefined_acl = 'projectPrivate')
    except Exception as e :
        logging.error("Error saving to Google Cloud Storage: {}\n\tError: {}"
            .format(gcsfilename, e))
    gcscsv.close()
    return gcsbasefilename


# Method to process s3 bucket, and save to cloud storage
# All files within the data directory will be process then moved to a done folder.
def processS3GCS():
    # Connect to S3
    s3client = boto3.resource('s3',
        aws_access_key_id=os.environ['s3accessid'].strip(),
        aws_secret_access_key=os.environ['s3secretkey'].strip()
        )
    s3bucketname = os.environ['s3bucketname'].strip()
    s3bucket = s3client.Bucket(s3bucketname)
    s3prefix = ''
    if (os.environ['s3path'].strip()):
        s3prefix = os.environ['s3path'].rstrip('/') + '/'
    segment_id = ''
    dateprefix = datetime.utcnow().strftime('%Y-%m-%d')
    segmentprefix = os.environ['brazesegmentid'].strip()
    s3prefix += "segment-export/{}/{}/".format(segmentprefix, dateprefix)
    s3doneprefix = os.environ['s3processedprefix'].strip()
    s3objs = s3bucket.objects.filter(Prefix=s3prefix)

    # Generate header an output string variable
    brazefields = [sf.strip() for sf in os.environ['brazesegmentfields'].split(',')]
    gcscsv = io.StringIO()
    csvwriter = csv.writer(gcscsv, delimiter=',')
    gcscsv.write(','.join(brazefields) + "\n")

    for s3obj in s3objs:
        if '.zip' in s3obj.key:
            buffer = io.BytesIO(s3obj.get()["Body"].read())
            with zipfile.ZipFile(buffer) as exports:
                for brazefiles in exports.infolist():
                    if '.txt' in brazefiles.filename:
                        with exports.open(brazefiles, 'r') as brazefile:
                            linecount = 0
                            for line in brazefile:
                                linecount += 1
                                try:
                                    line_utf8 = line.decode("utf-8").rstrip()
                                    record_json = json.loads(line_utf8)
                                    record = []
                                    for sfv in brazefields:
                                        if sfv in record_json:
                                            record.append(record_json[sfv])
                                        else:
                                            record.append(None)
                                    csvwriter.writerow(record)
                                    logging.info("Record {}".format(' '.join(str(x) for x in record)))
                                except Exception as e :
                                    logging.warning("Parse error in {} line {}: {}\n\tError: {}"
                                        .format(brazefiles.filename, linecount, line_utf8, e))
                                    continue
            # Move completed file to process folder
            s3client.Object(s3bucketname,s3obj.key.replace('segment-export',s3doneprefix)).copy_from(CopySource=s3bucketname + '/' + s3obj.key)
            s3client.Object(s3bucketname,s3obj.key).delete()

    # Create Google Cloud Storage Bucket file to save file to save to
    storage_client = storage.Client()
    gcsbucketname = os.environ['gcsproject'].lower().strip()
    gcsbucketname += '.appspot.com'
    gcsbucket = storage_client.get_bucket(gcsbucketname)
    gcsfilename = ''
    if (os.environ['gcspath']):
        gcsfilename += os.environ['gcspath'].strip() + '/'
    gcsbasefilename = "braze_export_{}".format(datetime.utcnow().strftime("%s"))
    gcsfilename += gcsbasefilename + '.csv'
    gcsfile = gcsbucket.blob(gcsfilename)
    content_type = 'text/csv'
    try:
        # Write file to cloud storage
        gcsfile.upload_from_string(
            gcscsv.getvalue(),
            content_type = content_type,
            predefined_acl = 'projectPrivate')
    except Exception as e :
        logging.error("Error saving to Google Cloud Storage: {}\n\tError: {}"
            .format(gcsfilename, e))
    gcscsv.close()
    return gcsbasefilename

# Creates temporary table linking to cloud storage file
def createTempTable(gscfile):
    gcsbucketname = os.environ['gcsproject'].lower().strip()
    gcsbucketname += '.appspot.com'
    gcsprefix = ''
    if (os.environ['gcspath']):
        gcsprefix += os.environ['gcspath'].strip() + '/'
    gcsfullurl = "gs://{}/{}{}.csv".format(gcsbucketname,gcsprefix,gscfile)
    gcsduration = int(os.environ['bigquery_temptable_duration'].strip())

    # set expiration of the table
    gcsexpiration = datetime.utcnow() + timedelta(seconds=gcsduration)
    bgqclient = bigquery.Client()
    bgqdataset = os.environ['bigquery_dataset'].strip()
    bgqdatasetid = bgqclient.dataset(bgqdataset)
    brazefields = [bf.lower().strip() for bf in os.environ['brazesegmentfields'].split(',')]
    brazetype = [bt.upper().strip() for bt in os.environ['brazesegmenttype'].split(',')]

    bgqschema = []
    # Generate schema based on field type
    for bf, bt in zip(brazefields, brazetype):
        bgqschema.append(bigquery.SchemaField(bf, bt, mode="NULLABLE"))

    table = bigquery.Table(bgqdatasetid.table(gscfile) , schema=bgqschema)
    table.expires = gcsexpiration
    external_config = bigquery.ExternalConfig("CSV")
    external_config.options.skip_leading_rows = 1
    external_config.source_uris = [gcsfullurl]
    table.external_data_configuration = external_config
    table = bgqclient.create_table(table)
    return table

# Method to merge temp table with master table
def mergeTables(gcstable):
    brazefielddml = os.environ['brazesegmentfields'].strip()
    brazefields = [bf.lower().strip() for bf in brazefielddml.split(',')]
    bgqdatasetid = os.environ['bigquery_dataset'].strip()
    bgqdest = os.environ['bigquery_table'].strip()
    bgqsource = gcstable.table_id
    brazeprimaryid = os.environ['gcsprimarykey'].strip()
    bqgmatched = []

    # Generate update dml string
    for bf in brazefields:
        if bf != brazeprimaryid:
            bqgmatched.append("dt.{} = st.{}".format(bf,bf))
    bgmatchedml = ', '.join(bqgmatched)

    # Generate upsert dml query
    bgqdml = """
        MERGE `{}`.`{}` dt
        USING `{}`.`{}` st
        ON dt.{} = st.{}
        WHEN MATCHED THEN
          UPDATE SET {}
        WHEN NOT MATCHED THEN
          INSERT({})
          VALUES({})
      """.format(
        bgqdatasetid, bgqdest,
        bgqdatasetid, bgqsource,
        brazeprimaryid,brazeprimaryid,
        bgmatchedml, brazefielddml, brazefielddml
        )
    bgqclient = bigquery.Client()
    query_job = bgqclient.query(bgqdml)
    results = query_job.result()

# Endpoint to process the callback response from the Braze export, post only
@app.route('/exportcallback', methods=['POST'])
def processBrazeCallBack():
    # Queue Braze Segment Export to be run.
    if not (request.is_json):
        logging.error("Segment Export Failure not json: {}".format(request.data))
        abort(401)
    else:
        payload = request.get_json()
        if not payload.keys() >= {"success"}:
            logging.error("Segment Export Failure missing response: {}".format(json.dumps(payload)))
            abort(400)
        # s3enabled = os.environ['s3enabled']
        if payload['success']:
            if ('s3enabled' in os.environ) and (str(os.environ['s3enabled']).strip().lower() in ['true', '1', 't', 'y', 'yes']):
                gscfile = processS3GCS()
            elif 'url' in payload:
                gscfile = processURLGCS(payload['url'])
            else:
                logging.error("Segment Export Failure missing response: {}".format(json.dumps(payload)))
                abort(400)
            # Create temporary table from Google Cloud Storage
            gcstemptable = createTempTable(gscfile.strip())
            # Merge Table
            mergeTables(gcstemptable)
            resp = Response('{"message": "ok"}', status=200, mimetype='application/json')
            return resp
        else:
            logging.error("Segment Export Failure not success: {}".format(json.dumps(payload)))
            abort(400)

@app.route('/')
def error():
    # Default page should never need to be used, so return bad request
    abort(401)

if __name__ == '__main__':
    # This is used when running locally only. When deploying to Google App
    # Engine, a webserver process such as Gunicorn will serve the app. This
    # can be configured by adding an `entrypoint` to app.yaml.
    app.run(host='127.0.0.1', port=8080, debug=True)

