from flask import Flask, render_template, request, Response, abort, jsonify
from google.cloud import bigquery, storage, tasks_v2

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
import gc

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

    gcsbasefilenames = []

    maxlinecount = 0
    filecount = 1
    gcsmaxlines = int(os.environ['gcsmaxlines'].strip())
    gcsfilename = ''

    gcspath = ''
    if (os.environ['gcspath']):
        gcspath = os.environ['gcspath'].strip() + '/'
    content_type = 'text/csv'

    gcscsv = io.StringIO()
    csvwriter = csv.writer(gcscsv, delimiter=',')
    gcscsv.write(','.join(brazefields) + "\n")
    with zipfile.ZipFile(io.BytesIO(brazeexport.content)) as exports:
        for brazefiles in exports.infolist():
            gc.collect()
            if '.txt' in brazefiles.filename:
                with exports.open(brazefiles, 'r') as brazefile:
                    linecount = 0
                    for line in brazefile:
                        linecount += 1
                        maxlinecount += 1
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
                        except Exception as e :
                            logging.warning("Parse error in {} line {}: {}\n\tError: {}"
                                .format(brazefiles.filename, linecount, line_utf8, e))
                            continue
                        # Chunk outputs into multiple files based on limit to avoid memory leaks
                        if maxlinecount >= gcsmaxlines:
                            try:
                                gcsbasefilename = "braze_export_{}_{}".format(datetime.utcnow().strftime("%s"),filecount)
                                gcsfilename = gcspath + gcsbasefilename + '.csv'
                                gcsfile = gcsbucket.blob(gcsfilename)
                                # Write file to cloud storage
                                gcsfile.upload_from_string(
                                    gcscsv.getvalue(),
                                    content_type = content_type,
                                    predefined_acl = 'projectPrivate')
                            except Exception as e :
                                logging.error("Error saving to Google Cloud Storage: {}\n\tError: {}"
                                    .format(gcsfilename, e))
                            # start a new batch
                            gcscsv.truncate(0)
                            gcscsv.seek(0)
                            gcsbasefilenames.append(gcsbasefilename)
                            filecount += 1
                            csvwriter = csv.writer(gcscsv, delimiter=',')
                            gcscsv.write(','.join(brazefields) + "\n")
                            maxlinecount = 0
            gc.collect()
    try:
        gcsbasefilename = "braze_export_{}_{}".format(datetime.utcnow().strftime("%s"),filecount)
        gcsfilename = gcspath + gcsbasefilename + '.csv'
        gcsfile = gcsbucket.blob(gcsfilename)
        # Write file to cloud storage
        gcsfile.upload_from_string(
            gcscsv.getvalue(),
            content_type = content_type,
            predefined_acl = 'projectPrivate')
        gcsbasefilenames.append(gcsbasefilename)
    except Exception as e :
        logging.error("Error saving to Google Cloud Storage: {}\n\tError: {}"
            .format(gcsfilename, e))

    gcscsv.close()
    return gcsbasefilenames


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

    # Create Google Cloud Storage Bucket file to save file to save to
    storage_client = storage.Client()
    gcsbucketname = os.environ['gcsproject'].lower().strip()
    gcsbucketname += '.appspot.com'
    gcsbucket = storage_client.get_bucket(gcsbucketname)
    gcsbasefilenames = []
    maxlinecount = 0
    filecount = 1
    gcsmaxlines = int(os.environ['gcsmaxlines'].strip())
    gcsfilename = ''

    gcspath = ''
    if (os.environ['gcspath']):
        gcspath = os.environ['gcspath'].strip() + '/'

    content_type = 'text/csv'

    # Generate header an output string variable
    brazefields = [sf.strip() for sf in os.environ['brazesegmentfields'].split(',')]
    gcscsv = io.StringIO()
    csvwriter = csv.writer(gcscsv, delimiter=',')
    gcscsv.write(','.join(brazefields) + "\n")
    for s3obj in s3objs:
        if '.zip' in s3obj.key:
            gc.collect()
            buffer = io.BytesIO(s3obj.get()["Body"].read())
            with zipfile.ZipFile(buffer) as exports:
                for brazefiles in exports.infolist():
                    if '.txt' in brazefiles.filename:
                        with exports.open(brazefiles, 'r') as brazefile:
                            linecount = 0
                            for line in brazefile:
                                linecount += 1
                                maxlinecount += 1
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
                                except Exception as e :
                                    logging.warning("Parse error in {} line {}: {}\n\tError: {}"
                                        .format(brazefiles.filename, linecount, line_utf8, e))
                                    continue
                                # Chunk outputs into multiple files based on limit to avoid memory leaks
                                if maxlinecount >= gcsmaxlines:
                                    try:
                                        gcsbasefilename = "braze_export_{}_{}".format(datetime.utcnow().strftime("%s"),filecount)
                                        gcsfilename = gcspath + gcsbasefilename + '.csv'
                                        gcsfile = gcsbucket.blob(gcsfilename)

                                        # Write file to cloud storage
                                        gcsfile.upload_from_string(
                                            gcscsv.getvalue(),
                                            content_type = content_type,
                                            predefined_acl = 'projectPrivate')
                                    except Exception as e :
                                        logging.error("Error saving to Google Cloud Storage: {}\n\tError: {}"
                                            .format(gcsfilename, e))
                                    # start a new batch
                                    gcscsv.truncate(0)
                                    gcscsv.seek(0)
                                    gcsbasefilenames.append(gcsbasefilename)
                                    filecount += 1

                                    csvwriter = csv.writer(gcscsv, delimiter=',')
                                    gcscsv.write(','.join(brazefields) + "\n")
                                    maxlinecount = 0
            buffer.truncate(0)
            buffer.seek(0)
            # Move completed file to process folder
            s3client.Object(s3bucketname,s3obj.key.replace('segment-export',s3doneprefix)).copy_from(CopySource=s3bucketname + '/' + s3obj.key)
            s3client.Object(s3bucketname,s3obj.key).delete()
            gc.collect()
    try:
        gcsbasefilename = "braze_export_{}_{}".format(datetime.utcnow().strftime("%s"),filecount)
        gcsfilename = gcspath + gcsbasefilename + '.csv'
        gcsfile = gcsbucket.blob(gcsfilename)

        # Write file to cloud storage
        gcsfile.upload_from_string(
            gcscsv.getvalue(),
            content_type = content_type,
            predefined_acl = 'projectPrivate')
        gcsbasefilenames.append(gcsbasefilename)
    except Exception as e :
        logging.error("Error saving to Google Cloud Storage: {}\n\tError: {}"
            .format(gcsfilename, e))
    gcscsv.close()
    return gcsbasefilenames

# Creates temporary table linking to cloud storage file
def createTempTable(gscfiles):
    gcsbucketname = os.environ['gcsproject'].lower().strip()
    gcsbucketname += '.appspot.com'
    gcsprefix = ''
    if (os.environ['gcspath']):
        gcsprefix += os.environ['gcspath'].strip() + '/'
    tables = []
    for gcf in gscfiles:
        gscfile = gcf.strip()
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
        tables.append(table)
    return tables

# Method to merge temp table with master table
def mergeTables(gcstables):
    brazefielddml = os.environ['brazesegmentfields'].strip()
    brazefields = [bf.lower().strip() for bf in brazefielddml.split(',')]
    bgqdatasetid = os.environ['bigquery_dataset'].strip()
    bgqdest = os.environ['bigquery_table'].strip()
    for gcstable in gcstables:
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

# Endpoint to process tasks queues
@app.route('/processtask', methods=['POST'])
def processTask():
    payload_text = request.get_data(as_text=True) or ''
    payload = {}
    try:
        payload = json.loads(payload_text)
    except Exception as e:
        pass
    if ('s3enabled' in os.environ) and (str(os.environ['s3enabled']).strip().lower() in ['true', '1', 't', 'y', 'yes']):
        gscfiles = processS3GCS()
    elif 'url' in payload:
        gscfiles = processURLGCS(payload['url'])
    else:
        logging.error("Segment Export Failure missing response: {}".format(json.dumps(payload)))
        abort(400)
    # Create temporary table from Google Cloud Storage
    gcstemptables = createTempTable(gscfiles)
    # Merge Table
    mergeTables(gcstemptables)
    resp = Response('{"message": "ok"}', status=200, mimetype='application/json')
    return resp

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
        if payload['success']:
            if ('gcsusetask' in os.environ) and (str(os.environ['gcsusetask']).strip().lower() in ['true', '1', 't', 'y', 'yes']):
                client = tasks_v2.CloudTasksClient()
                parent = client.queue_path(os.environ['gcsproject'].strip().lower(), os.environ['gcslocation'].strip(), os.environ['gcstaskqueue'].strip().lower())
                payload = request.get_json()
                payload_url = '{}'
                if 'url' in payload:
                    payload_url = '{{"url":"{}"}}'.format(payload['url'])
                task = {
                    'app_engine_http_request': {
                        'http_method': 'POST',
                        'relative_uri': '/processtask',
                        'body': payload_url.encode()
                    }
                }
                response = client.create_task(parent, task)
            else:
                if ('s3enabled' in os.environ) and (str(os.environ['s3enabled']).strip().lower() in ['true', '1', 't', 'y', 'yes']):
                    gscfiles = processS3GCS()
                elif 'url' in payload:
                    gscfiles = processURLGCS(payload['url'])
                else:
                    logging.error("Segment Export Failure missing response: {}".format(json.dumps(payload)))
                    abort(400)
                # Create temporary table from Google Cloud Storage
                gcstemptables = createTempTable(gscfiles)
                # Merge Table
                mergeTables(gcstemptables)

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

