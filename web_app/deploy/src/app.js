'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);
const bucket = 'dasnes-mpcs53014'
var fs = require('fs');
var path = require('path');
var AWS = require('aws-sdk');
const {
  TranscribeClient,
  StartTranscriptionJobCommand,
} = require("@aws-sdk/client-transcribe");

const transcriber = new TranscribeClient({region: 'us-east-2', accessKeyId : "AKIA3OT2CZXEDVROT2PF", secretAccessKey : "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"});
AWS.config.update({region: 'us-east-2', accessKeyId : "AKIA3OT2CZXEDVROT2PF", secretAccessKey : "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"});
var s3 = new AWS.S3();
const multer = require('multer');
const prefix = 'uploads/';
const upload = multer({
  dest: prefix // this saves your file into a directory called "uploads"
});
const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})
const hbaseTableName = 'dasnes_view_as_hbase';
// HBase counters are stored as 8 byte binary data that the HBase Node module
// interprets as an 8 character string. Use the Javascript Buffer library to
// convert into a number
function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}

var htmlViewTop = `<!DOCTYPE html PUBLIC "-//IETF//DTD HTML 2.0//EN">
<html>
  <head>
    <title>Views</title>
    <link type="text/css" rel="stylesheet" href="table.css" />
  </head>
  <body>
  <h1>View profile of speech recordings</h1>
  <div>
  <form action="/getView" method="get" style="background:#FFFFFF;width:60%;margin:auto" class="elegant-aero">
  `;

var htmlViewBottom = `<button type="submit">Submit</button></form></div>
<div><br><button onclick="window.location.href = '/submit.html'">Upload your own police interaction</button></div>
</body></html>`;


var possibleDepts;
var possibleZones;
var possibleTimeOfDay;
var possibleSeason;

function generateHtmlFilters(){
	//first add dept filter
	var deptFilter = `<select name="dept" id="dept">`;
	// <option value="any_pd">Any PD</option>`;
	possibleDepts.forEach(d => {
		deptFilter += `<option value="` + d + `" id="` + d + `">` + d + `</option>`
	});
	deptFilter += `</select>`;

	var zoneFilter = `<select name="zone" id="zone">`;
	// <option value="any_zone">Any Zone</option>`;
	possibleZones.forEach(z => {
		zoneFilter += `<option value="` + z + `" id="` + z + `">` + z + `</option>`
	});
	zoneFilter += `</select>`;

	var timeOfDayFilter = `<select name="time_of_day" id="time_of_day">`;
	// <option value="any_time_of_day">Any Time of Day</option>`;
	possibleTimeOfDay.forEach(z => {
		timeOfDayFilter += `<option value="` + z + `" id="` + z + `">` + z + `</option>`
	});
	timeOfDayFilter += `</select>`;

	var seasonFilter = `<select name="time_of_year" id="time_of_year">`;
	// <option value="any_time_of_year">Any Time of Year</option>`;
	possibleSeason.forEach(z => {
		seasonFilter += `<option value="` + z + `" id="` + z + `">` + z + `</option>`
	});
	seasonFilter += `</select>`;

	return deptFilter + zoneFilter + timeOfDayFilter + seasonFilter;
}

var formViewHtml;
//to dynamically populate dropdown with validate options in hbase
hclient.table(hbaseTableName).
	scan({
		maxVersions: 1},
		(err, cells) => {
			if (!cells) {
				console.log("no cells came back on page load!");
				formViewHtml = "<p>No cells came back on initial page load :(</p>";
				return;
			}

			possibleDepts = Array.from(new Set(cells.
				filter(x => x.column === 'stats:dept_name').
				map(x => x['$'])));
			possibleZones = Array.from(new Set(cells.
				filter(x => x.column === 'stats:zone').
				map(x => x['$'])));
			possibleTimeOfDay = Array.from(new Set(cells.
				filter(x => x.column === 'stats:time_of_day').
				map(x => x['$'])));
			possibleSeason = Array.from(new Set(cells.
				filter(x => x.column === 'stats:season').
				map(x => x['$'])));
			
			//now generate dynamic html
			formViewHtml = htmlViewTop + generateHtmlFilters() + htmlViewBottom;
		});


function generateHbaseRowKeyFromViewRequest(req){
	//first extract args from request
	//and build key used to query hbase
	var dept = req.query['dept'];
	var zone = req.query['zone'];
	var timeOfDay = req.query['time_of_day'];
	var timeOfYear = req.query['time_of_year'];

	return dept+zone+timeOfDay+timeOfYear;
}

var resultViewHtml = filesystem.readFileSync("resultView.mustache").toString();

app.use(express.static('public'));
app.get('/', function (req, res) {
	//loads the main screen, so display prepopulated filter options
	res.send(formViewHtml);
});

app.get('/getView', (req, res) => {
	var key = generateHbaseRowKeyFromViewRequest(req);
	hclient.table(hbaseTableName).row(key).get((err, cells) => {
		if (!cells) {
			res.send("<p>sorry but the hbase query failed :(</p>");
			return;
		} 

		var mostCommonWords = cells.
			filter(x => x.column === 'stats:most_common_words').
			map(x => x['$'])[0].
			split(","); //map returns a list so we take the first (only) elem of that, then split on ","
		
		var leastCommonWords = cells.
			filter(x => x.column === 'stats:least_common_words').
			map(x => x['$'])[0].
			split(","); //map returns a list so we take the first (only) elem of that, then split on ","
		
		var sentimentScoreSum = cells.
			filter(x => x.column === 'stats:sentiment_score_sum').
			map(x => x['$'])[0];
		sentimentScoreSum = counterToNumber(sentimentScoreSum);

		var sentimentScoreTotal = cells.
			filter(x => x.column === 'stats:sentiment_score_total').
			map(x => x['$'])[0];
		sentimentScoreTotal = counterToNumber(sentimentScoreTotal);

		var html = mustache.render(resultViewHtml, {
			mcw: mostCommonWords.join(", "),
			lcw: leastCommonWords.join(", "),
			ss: ((sentimentScoreSum/sentimentScoreTotal)*100.0).toFixed(2).toString() + "%"
		});
		res.send(html);
	})
});

function buildAupKey(date, time, deptName, zone, doesHaveAudio){

	// the key to json files is required to encode certain information
	// for the kafka consumer to process it
	// so the "aupKey" as we call it 
	// needs to be formatted like: DDMMYYYYHHMMSS.dept.zone.audioIndicator.uuid

	// date is in the form 2020-12-09
	// but we want it to be DDMMYYYY
	date = date.replaceAll('-', '');
	var dateString = date.substring(6, 8) + date.substring(4, 6) + date.substring(0, 4);

	//time is in the form HH:MM and we want it as HHMMSS
	time = time.replaceAll(':', '');
	var timeString = time + "00";
	
	// kafka consumer needs to know if has audio or not in key
	// to know the fields of the json that it will parse
	var audioIndicator = (doesHaveAudio) ? "yesAudio" : "noAudio";

	return dateString + timeString + "." + deptName + "." + zone + "." + audioIndicator + "." + String(Date.now());
}

var kafka = require('kafka-node');
var Producer = kafka.Producer;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);

app.post('/writeData', upload.single('recording'), function (req, res) {
	// we send our response immediately because our processing can be slow
	res.redirect('submit.html');

	// process user input from request
	var deptName = (req.body['deptName']) ? req.body['deptName'] : null;
	var zone = (req.body['zone']) ? req.body['zone'] : null;
	var date = (req.body['date']) ? req.body['date'] : null;
	var time = (req.body['time']) ? req.body['time'] : null;
	var duration = (req.body['duration']) ? req.body['duration'] : null;
	var text = (req.body['text']) ? req.body['text'] : null ;

	var aupKey = buildAupKey(date, time, deptName, zone, req.file);

	var report = {
		id : aupKey,
		dept_name : deptName,
		zone : zone,
		date : date,
		time : time,
		duration: duration,
		text : text,
		recording: ((req.file) ? true : false) 
	};

	// as of now we can't process if any of the required fields are null
	if (!report.id || !report.dept_name || !report.zone || !report.date || !report.time || !report.duration || !report.text){
		// the need for no nulls is now probably a relic of debugging the second spark context exception in spark-submit inference job
		console.log("sentiment inference in kafka consumer doesn't yet support report with any null fields.");
		return;
	}

	if (!req.file) {
		s3.putObject({
			Body: JSON.stringify(report),
			Bucket: bucket,
			Key: aupKey + ".json"
		}, (err, data) => {
			console.log("uploaded json file: " + aupKey + ".json");
			console.log(err);
			console.log(data);
		});

		var kafkaObj = {"key": aupKey, "body": text};
		//also write the text to a kafka topic
		kafkaProducer.send([{ topic: 'topic_dasnes_json_reached_s3', messages: JSON.stringify(kafkaObj)}],
			function (err, data) {
				console.log("no audio file in upload");
				console.log("Kafka Error: " + err);
				console.log(data);
				console.log(report);
			});

	} else {
		//upload audio file to s3
		var filepath = prefix + req.file.filename;
		var fileStream = fs.createReadStream(filepath);
		fileStream.on('error', function(err) {
			console.log('File Error', err);
		});		 
		var audioUploadParams = {Bucket: bucket, Key: aupKey + ".mp3", Body: fileStream};

		s3.upload(audioUploadParams, function (err, data) {
			if (err) console.log("Error", err);
			if (data) {
				console.log("Uploaded in:", data.Location);
				//now can delete the origin file from our local filesystem
				filesystem.unlink(filepath, (err) => {
					if (err) {
						console.log(err);
						console.log("failed to remove file at path: " + filepath);
					}
				});

				const params = {
					TranscriptionJobName: aupKey,
					LanguageCode: 'en-US',
					MediaFormat: "mp3", // TODO: make this generic
					Media: {
						MediaFileUri: "s3://" + bucket + "/" + aupKey + ".mp3"
					},
					OutputBucketName: bucket
				};

				transcriber.send(new StartTranscriptionJobCommand(params), (data, err) => {
					console.log(err);
					console.log(data);
				});
			} else {
				console.log("no data after trying to upload audio to s3. returning html page");
			}
		});
	}
});

app.listen(port);
