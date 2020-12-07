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
AWS.config.update({region: 'us-east-2', accessKeyId : "AKIA3OT2CZXEDVROT2PF", secretAccessKey : "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"});
var s3 = new AWS.S3();
const multer = require('multer');
const prefix = 'uploads/';
const upload = multer({
  dest: prefix // this saves your file into a directory called "uploads"
});
const hbase = require('hbase')
var hclient = hbase({ host: process.argv[3], port: Number(process.argv[4])})

// HBase counters are stored as 8 byte binary data that the HBase Node module
// interprets as an 8 character string. Use the Javascript Buffer library to
// convert into a number
function counterToNumber(c) {
	return Number(Buffer.from(c).readBigInt64BE());
}
function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = counterToNumber(item['$'])
	});
	return stats;
}

hclient.table('dasnes_proj_csv_as_hbase').row('12345').get((error, value) => {
	// console.info(rowToMap(value))
	console.info(value)
})


app.use(express.static('public'));
app.get('/delays.html',function (req, res) {
    const route=req.query['origin'] + req.query['dest'];
    console.log(route);
	hclient.table('weather_delays_by_route_v2').row(route).get(function (err, cells) {
		const weatherInfo = rowToMap(cells);
		console.log(weatherInfo)
		function weather_delay(weather) {
			var flights = weatherInfo["delay:" + weather + "_flights"];
			var delays = weatherInfo["delay:" + weather + "_delays"];
			if(flights == 0)
				return " - ";
			return (delays/flights).toFixed(1); /* One decimal place */
		}

		var template = filesystem.readFileSync("result.mustache").toString();
		var html = mustache.render(template,  {
			origin : req.query['origin'],
			dest : req.query['dest'],
			clear_dly : weather_delay("clear"),
			fog_dly : weather_delay("fog"),
			rain_dly : weather_delay("rain"),
			snow_dly : weather_delay("snow"),
			hail_dly : weather_delay("hail"),
			thunder_dly : weather_delay("thunder"),
			tornado_dly : weather_delay("tornado")
		});
		res.send(html);
	});
});

/* Send simulated weather to kafka */
var kafka = require('kafka-node');
var Producer = kafka.Producer;
var KeyedMessage = kafka.KeyedMessage;
var kafkaClient = new kafka.KafkaClient({kafkaHost: process.argv[5]});
var kafkaProducer = new Producer(kafkaClient);
var counter = 0;

app.post('/writeData', upload.single('recording'), function (req, res) {
	
	var deptName = (req.body['deptName']) ? req.body['deptName'] : null;
	var zone = (req.body['zone']) ? req.body['zone'] : null;
	var date = (req.body['date']) ? req.body['date'] : null;
	var time = (req.body['time']) ? req.body['time'] : null;
	var duration = (req.body['duration']) ? req.body['duration'] : null;
	var text = (req.body['text']) ? req.body['text'] : null ;
	var report = {
		zone_timestamp : "fromclient" + counter,
		dept_name : deptName,
		zone : zone,
		date : date,
		time : time,
		duration: duration,
		text : text,
		recording: null
	};
	console.log(report);
	counter = counter + 1;
	if (req.file) {
		//upload audio file to s3
		var filepath = prefix + req.file.filename;
		console.log(filepath);
		var fileStream = fs.createReadStream(filepath);
		fileStream.on('error', function(err) {
			console.log('File Error', err);
		});		  
		var uploadParams = {Bucket: bucket, Key: '', Body: ''};
		uploadParams.Body = fileStream;
		uploadParams.Key = "";
		if (deptName) uploadParams.Key += deptName;
		if (zone) uploadParams.Key += zone;
		if (date) uploadParams.Key += date;
		if (time) uploadParams.Key += time;
		uploadParams.Key += "." + Date.now() + ".mp3";
		console.log(uploadParams.Key)
		report.recording = uploadParams.Key;
		s3.upload(uploadParams, function (err, data) {
			if (err) console.log("Error", err);
			if (data) {
				console.log("Uploaded in:", data.Location);
				//now can delete the origin file from our local filesystem
				filesystem.unlink(filepath, (err) => {
					if (err) {
						console.log(err);
						console.log("failed to remove file at path: " + filepath);
					}
				})

				//now post to kafka topic that audio was uploaded
				kafkaProducer.send([{ topic: 'topic_dasnes_web_upload_with_audio', messages: JSON.stringify(report)}],
					function (err, data) {
						console.log("post to kafka after successful upload to s3");
						console.log("Kafka Error: " + err)
						console.log(data);
						console.log(report);
					});
			} else {
				console.log("no data after trying to upload audio to s3. returning html page");
			}
		});

	} else {
		kafkaProducer.send([{ topic: 'topic_dasnes_web_upload_no_audio', messages: JSON.stringify(report)}],
			function (err, data) {
				console.log("no audio file in upload");
				console.log("Kafka Error: " + err)
				console.log(data);
				console.log(report);
			});
	}

	// regardless of conditionals, all branches must upload report to the day's new folder in s3
	// so that it can get ingested into the mdc 
	let date_ob = new Date();
	let cur_date = ("0" + date_ob.getDate()).slice(-2); // adjust 0 before single digit date
	let month = ("0" + (date_ob.getMonth() + 1)).slice(-2); // current month
	let year = date_ob.getFullYear(); // current year
	var folder_date_prefix = "INGESTION" + year + "-" + month + "-" + cur_date + "/";

	s3.putObject({
		Bucket: bucket,
		Key: folder_date_prefix + Date.now() + ".json",
		Body: JSON.stringify(report),
		ContentType: "application/json"},
		function (err,data) {
		  console.log(JSON.stringify(err) + " " + JSON.stringify(data));
		}
	  );

	res.redirect('submit-weather.html');
});

app.listen(port);
