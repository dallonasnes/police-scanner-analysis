# Realtime police scanner analysis
## Big data web app deployed to AWS EMR Cluster

### Overview
This project performs real-time speech content analyses on audio recordings of police by constantly listening to the public police scanners in various districts occupied by Chicago Police Department and also supporting manually uploaded recordings in a web application.

It is an implementation of the lambda architecture. Batch layer processing includes processing source truth data - transcripts of police scanner recordings and metadata - in Hive and analyzing it in Spark. The serving layer is implemented with hbase and serves precomputed views to a node.js web server that then renders views on a web client. The speed layer includes a pipeline managing real-time transcription of new audio recordings, lambda functions that send messages to kafka topics when transcription of a recording has finished, and a Spark kafka-consumer that runs Sentiment Analysis inference on the transcriptions of recordings and updates statistics in hbase in real time.

Demo video and walkthrough at this link: https://youtu.be/PHhb9FtWUL8

### How to run on AWS EMR Cluster
Note: The cluster this was deployed to has since been taken down. The below instructions may be helpful for those wishing to deploy this code on a new cluster.

#### Running deployed code
Node web app is already deployed to load balanced web servers. You can reach the home page at `http://mpcs53014-loadbalancer-217964685.us-east-2.elb.amazonaws.com:3005`

The node web app allows users to query precomputed analyses of police language and can be filtered by police dept, zone/beat, time of day and time of year. This app demos basic analyses - word count applied to find most and least common words, and a Logistic Regression model inferring sentiment scores from the transcripts of processed recordings.

Kafka consumer uber jar lives at `/home/hadoop/dasnes/web-text-input-kafka-consumer/target`. `cd` there and run `spark-submit --master local[2] --driver-java-options "-Dlog4j.configuration=file:///home/hadoop/ss.log4j.properties" --class ProcessLiveWebInput uber-web-text-input-kafka-consumer-1.0-SNAPSHOT.jar b-1.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092,b-2.mpcs53014-kafka.fwx2ly.c4.kafka.us-east-2.amazonaws.com:9092`

The kafka consumer listens to a topic that receives S3 URIs of json files containing the results of transciptions of streamed and uploaded audio input for processing. The spark task parses the .json file from S3 and feeds the transcript into the sentiment analysis model for inference. It then uses metadata from the file to identify the correct row in hbase and increments average sentiment score counters accordingly.

Note that issues in spark-submit job mode repeatedly cause a null sparkContext, relevant to getting files from S3 and doing inference. The accompanying video demonstrates this functionality in a spark shell. The version deployed to the cluster "feigns" the inference functionality by just uploading the count of recordings in the hbase table as they are processed uploaded to the web client. Running the job using spark-shell would support speed layer real time updates to hbase based on scanner input stream as well.

#### Running forever threads to listen to scanner broadcast
These can be run from any machine, so long as it has access keys to the S3 bucket. Note that a single 24hr/daystream of one CPD district downloads mere megabytes of .mp3 files per day, so a single machine can support listeners to many urls.

To run, `cd stream_listeners/` and then run `pip3 -r requirements.txt` Run `python ingest_audio.py <broadcastify url> <dept info> <zone info>` to kick of a 1 minute listener job.

Jobs can be scheduled with a crontab command to support running a job every minute and adding more jobs. I ran a single listener locally with this command, although adding more commands in parallel supports adding more input scanner streams:
`*/1 * * * * cd ~/Develop/police-scanner-analysis/IngestionPipeline/stream_listeners && $(which python) ingest_audio.py https://audio.broadcastify.com/32936.mp3 cpd zone1 >> logs.txt`

The listener jobs work by, after downloading from the audio stream for the specified batch period, kicking off a AWS transcribe job to process the saved audio file.

#### Triggering Lambda function
A lambda function writes a .json file S3 URI to a kafka topic every time a .json file is created in S3. A new json file in S3 means that either a user has made an upload in the web client (text or audio input) that has been processed, or a batch of audio from a listener thread has been processed.

As described above, this lambda function produces messages for the kafka topic consumed by the `kafka_consumer/` project.

#### Training a Sentiment Analysis Model
`SentimentAnalysisModelTrainingPipeline.scala` in the `training_pipeline/` directory can be run in spark-shell. It trains a logistic regression model on an imdf movie review dataset that I put in hdfs, and saves the trained model to hdfs.


#### Loading and appending to source truth data
Hql files used to load the initial data set into Hive and Hbase can be found in `batch_pipeline/`. `process_and_load_ingested_data.sh` is a "pseudo-script" demonstraing how to build the hive and hbase tables from the source truth data.

#### Work in progress
Now that the basic design has been implemented, lots of things can be done to improve the project. One that I got started on is attempts to reduce the cost of AWS Transcribe for transcription by filtering long silences out of mp3 audio before sending it to AWS for transcription. I began implementing this addition to the ingestion pipeline, but have not had time to complete it. You can take a look at my progress in `stream_listeners/future_work/`. Note that there are a lot of ffmpeg dependencies that I was best able to manage with Docker.


#### Future Todo if I had more time
Configuration on cluster:
1. Set up ingestion script to append daily new batch of input data from S3 into source truth in hive. This would also require moving said new batch of data into an S3 archive after it has been validated and appended to the source truth data set. 
