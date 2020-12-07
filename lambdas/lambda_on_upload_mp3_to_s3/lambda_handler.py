import json
import urllib.parse
import boto3
import time

print('Loading function')

s3 = boto3.client('s3')
transcribe = boto3.client('transcribe')


def lambda_handler(event, context):
    #print("Received event: " + json.dumps(event, indent=2))

    # Get the object from the event and show its content type
    bucket = event['Records'][0]['s3']['bucket']['name']
    key = urllib.parse.unquote_plus(event['Records'][0]['s3']['object']['key'], encoding='utf-8')
    print(key)
    try:
        response = s3.get_object(Bucket=bucket, Key=key)
        
        job_uri = "s3://dasnes-mpcs53014/" + key
        job_name = (key.split('.')[0]).replace(" ", "") + str(time.time())
        
        transcribe.start_transcription_job(
            TranscriptionJobName=job_name,
            Media={'MediaFileUri': job_uri},
            MediaFormat=key.split('.')[1],
            LanguageCode='en-US',
            Settings = {'ShowSpeakerLabels': True,
                      'MaxSpeakerLabels': 4
                      },
            OutputBucketName='dasnes-mpcs53014',
            OutputKey=job_name + ".json"
        )
        
        print("KEY is: " + key)
        print("CONTENT TYPE: " + response['ContentType'])
        print(response)
        return response['ContentType']
    except Exception as e:
        print(e)
        print('Error getting object {} from bucket {}. Make sure they exist and your bucket is in the same region as this function.'.format(key, bucket))
        raise e
