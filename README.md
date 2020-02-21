# Braze User Export to Google BigQuery
This is an Google Cloud AppEngine python 3 script which automates the process of making getting Braze data to Google BigQuery.

_Only standard attributes and data types(String, Number) has been tested_

The following permission and Google Services are required:
* [Google BigQuery](https://console.cloud.google.com/bigquery)
* [Google API & Services](https://console.cloud.google.com/apis/dashboard) enabled
* [Google Cloud Storage](https://console.cloud.google.com/storage/)
* [Google App Engine](https://console.cloud.google.com/appengine)
* Braze API Key with [users.export.segment](https://www.braze.com/docs/api/endpoints/export/user_data/post_users_segment/) permissions
* Access to [Amazon AWS S3](https://console.aws.amazon.com/console/) (Required only if S3 Exports are setup)

**Note: Ensure BigQuery and Cloud Storage are running from the same geo-location to avoid issues**

## Process Steps
The following is an outline of the process:
* An API call to the [Braze User by Segment](https://www.braze.com/docs/api/endpoints/export/user_data/post_users_segment/) endpoint using a predefined [segment](https://www.braze.com/docs/user_guide/engagement_tools/segments/creating_a_segment/) with a callback to the app.
* Waits for Braze to trigger the callback.
	* If S3 for exports is enabled, reads from S3 (**TO BE IMPLEMENTED**).
	* Otherwise, pulls info from a zip S3 url.
* Converts files to a csv, and uploads to Google Cloud Storage.
* Create temporary BigQuery table off the csv file.
* Merge the temporary table with master table.

![BrazeBigQueryProcess](/img/BrazeBigQuery.png)

## Deploy to Google Cloud App Engine
To deploy to your [Google Cloud Project](https://cloud.google.com/sdk/gcloud/reference/app/deploy), clone this report locally or via [Google Cloud Shell](https://ssh.cloud.google.com/cloudshell/editor).

```
git clone git@github.com:Appboy/brazebigqueryexport.git
```

Create an `app.yaml`, see [app_example.yaml](/app_example.yaml) and deploy to your project using [gcloud cli](https://cloud.google.com/sdk/gcloud).
```
gcloud app deploy
```

### app.yaml
To set the environment variables for the script to run with, make an `app.yaml` (see [app_example.yaml](/app_example.yaml)) file in the root directly and updated the following:

```
env_variables:
	gcsproject: [Google Project Name]
	bigquery_dataset: [BigQuery DataSet]
	bigquery_table: [BigQuery Destination Table]
	bigquery_temptable_duration: [BigQuery TempTable Expiration (Seconds)]
	brazerestendpoint: [Braze API REST Endpoint ie https://rest.braze.com/]
	brazeapikey: [Braze API Key with User Segment Export Permissions]
	brazesegmentid: [Braze Segment ID]
	brazesegmentendpoint: [Braze API Endpoint ie /users/export/segment]
	brazesegmentfields: [Braze export fields ie external_id,random_bucket,first_name]
	brazesegmenttype: [Braze export field type ie STRING,INTEGER,STRING]
	gcsprimarykey: [BigQuery primary key external_id]
	gcsplatformprefix: [Google Appengine URL (Optional) ie .appspot.com]
	gcspath: [Google Cloud Store path ie brazeexport]
	s3enabled: [Boolean if AWS S3 is used]
	s3accessid: [AWS Access ID]
	s3secretkey: [AWS Secret Key]
	s3bucketname: [AWS Bucket Name]
	s3path: [AWS Bucket Prefix, optional]
```

#### app.yaml example
Example:
```
env_variables:
	gcsproject: BrazeBigQuery
	bigquery_dataset: bgdataset
	bigquery_table: mastertable
	bigquery_temptable_duration: 86400
	brazerestendpoint: https://rest.iad-01.braze.com/
	brazeapikey: api-key-with-user-export-segment-permission
	brazesegmentid: segmentidfromsegmentcreation
	brazesegmentendpoint: /users/export/segment
	brazesegmentfields: external_id,random_bucket,first_name
	brazesegmenttype: STRING,INTEGER,STRING
	gcsprimarykey: external_id
	gcsplatformprefix: .appspot.com
	gcspath: brazeexport
	s3enabled: true
	s3accessid: aws-access-id
	s3secretkey: aws-secret-key
	s3bucketname: bucket-name
	s3path: brazeexports
```

### cron.yaml
A [cron job](https://cloud.google.com/appengine/docs/flexible/python/scheduling-jobs-with-cron-yaml) can be tasked to trigger the Braze API Export using the internal `/schedule` endpoint. See [cron.yml](/cron.yaml) for example.
```
cron:
- description: "schedule hourly processing of Braze exports"
  url: /schedule
  schedule: every 1 hours
  retry_parameters:
    job_retry_limit: 2
    min_backoff_seconds: 2.5
    max_backoff_seconds: 10
    max_doublings: 3
```

To deploy run:
```
gcloud app deploy cron.yaml
```

Any updates to the `cron.yaml` will require the `cron.yaml` file to be redeployed.

