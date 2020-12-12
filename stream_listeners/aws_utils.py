import time

import boto3
from botocore.exceptions import NoCredentialsError
import pandas as pd
from secrets import AWS_ACCESS_KEY as ACCESS_KEY, AWS_SECRET_KEY as SECRET_KEY

def upload_to_aws(local_file, bucket, s3_file):
    s3 = boto3.client('s3', aws_access_key_id=ACCESS_KEY,
                      aws_secret_access_key=SECRET_KEY)

    try:
        s3.upload_file(local_file, bucket, s3_file)
        print("Upload Successful")
        return True
    except FileNotFoundError:
        print("The file was not found")
        return False
    except NoCredentialsError:
        print("Credentials not available")
        return False

def get_transcriber():
    transcribe = boto3.client('transcribe',
                            aws_access_key_id=ACCESS_KEY,
                            aws_secret_access_key=SECRET_KEY,
                            region_name="us-east-2"
    )
    return transcribe

#methods inspired by: https://colab.research.google.com/drive/1oaS1dOj5kkzx9Q8YRZd54AGHzrQEqg_9#scrollTo=RNfgzRWvrwBq
def check_job_name(transcribe, job_name):
  job_verification = True

  # all the transcriptions
  existed_jobs = transcribe.list_transcription_jobs()

  for job in existed_jobs['TranscriptionJobSummaries']:
    if job_name == job['TranscriptionJobName']:
      job_verification = False
      break

  if not job_verification:
    command = input(job_name + " has existed. \nDo you want to override the existed job (Y/N): ")
    if command.lower() == "y" or command.lower() == "yes":
      transcribe.delete_transcription_job(TranscriptionJobName=job_name)
    elif command.lower() == "n" or command.lower() == "no":
      job_name = input("Insert new job name? ")
      check_job_name(transcribe, job_name)
    else: 
      print("Input can only be (Y/N)")
      command = input(job_name + " has existed. \nDo you want to override the existed job (Y/N): ")
  return job_name

def amazon_transcribe(transcribe, audio_file_name, max_speakers=-1):    
  if max_speakers > 10:
    raise ValueError("Maximum detected speakers is 10.")

  job_uri = "s3://dasnes-mpcs53014/" + audio_file_name
  job_name = audio_file_name.replace(".mp3", "")#(audio_file_name.split('.')[0]).replace(" ", "") + str(time.time())
  
  # check if name is taken or not
  job_name = check_job_name(transcribe, job_name)
  
  if max_speakers != -1:
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat='mp3',#audio_file_name.split('.')[1],
        LanguageCode='en-US',
        Settings={'ShowSpeakerLabels': True,
                    'MaxSpeakerLabels': max_speakers
                  },
        OutputBucketName='dasnes-mpcs53014'
    )
  else: 
    transcribe.start_transcription_job(
        TranscriptionJobName=job_name,
        Media={'MediaFileUri': job_uri},
        MediaFormat='mp3', #audio_file_name.split('.')[1],
        LanguageCode='en-US',
        Settings={'ShowSpeakerLabels': True,
                  'MaxSpeakerLabels': 2
                  },
        OutputBucketName='dasnes-mpcs53014'
    )


  time.sleep(1)
  return transcribe.get_transcription_job(TranscriptionJobName=job_name)

if __name__ == "__main__":
      pass
      #test_filename = input("enter file path to upload")
      #uploaded = upload_to_aws("data/transcripts/starter_data_final_schema.csv", 'dasnes-mpcs53014', 'starter_data_final_schema.csv')
      #print(uploaded)
      #transcriber = get_transcriber()
      #res = amazon_transcribe(transcriber, "zone1.mp3", max_speakers=2)
    
