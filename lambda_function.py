import venv.codecs as codecs
import venv.csv as csv
import urllib.parse
import boto3
import venv.pandas_redshift as pr
import venv.pandas as pd

s3 = boto3.resource('s3',
                    aws_access_key_id="aws_access_key_id",
                    aws_secret_access_key="aws_secret_access_key")

pr.connect_to_redshift(dbname='dbname',
                                       host='host',
                                       port=5439,
                                       user='user',
                                       password='password'
                                       )
print('Loading function')


def lambda_handler(event, context):
    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    try:
        print(f'blob_path: s3://{bucket}/{key}')
        obj = s3.Object(bucket, key).get()['Body']

        # DictReader is a generator; not stored in memory
        file = csv.DictReader(codecs.getreader('cp850')(obj))
        df = pd.DataFrame(file)
        df['created_at'] = pd.to_datetime(df['timestamp'], unit='ms')
        df = df.drop(columns='timestamp')

        #
        list_columns = df.columns.values.tolist()
        str_columns = ', '.join([str(elem) for elem in list_columns])

        pr.connect_to_s3(aws_access_key_id="aws_access_key_id",
                         aws_access_key_id="aws_access_key_id",
                         bucket="event-data",
                         subdirectory="temp")

        # Write the DataFrame to S3 and then to redshift
        pr.pandas_to_redshift(data_frame=df, redshift_table_name=f'event ({str_columns})', append=True)
        pr.close_up_shop()

        return True

    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
