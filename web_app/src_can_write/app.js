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
const hbaseTableName = 'dasnes_view_as_hbase';
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

var htmlViewTop = `<!DOCTYPE html PUBLIC "-//IETF//DTD HTML 2.0//EN">
<html>
  <head>
    <title>Views</title>
    <link type="text/css" rel="stylesheet" href="table.css" />
  </head>
  <body>
  <div></div>`;

var htmlViewBottom = `</div>
</body></html>`;


var possibleDepts;
var possibleZones;
var possibleTimeOfDay;
var possibleSeason;

function generateHtmlFilters(){
	//first add dept filter
	var deptFilter = `<select name="dept" id="dept">
		<option value="any_pd">Any PD</option>`;
	possibleDepts.forEach(d => {
		deptFilter += `<option value="` + d + `" id="` + d + `">` + d + `</option>`
	});
	deptFilter += `</select>`;

	var zoneFilter = `<select name="zone" id="zone">
		<option value="any_zone">Any Zone</option>`;
	possibleZones.forEach(z => {
		zoneFilter += `<option value="` + z + `" id="` + z + `">` + z + `</option>`
	})
	zoneFilter += `</select>`;

	var timeOfDayFilter = `<select name="time_of_day" id="time_of_day">
	<option value="any_time_of_day">Any Time of Day</option>`;
	possibleTimeOfDay.forEach(z => {
		timeOfDayFilter += `<option value="` + z + `" id="` + z + `">` + z + `</option>`
	})
	timeOfDayFilter += `</select>`;

	var seasonFilter = `<select name="time_of_year" id="time_of_year">
	<option value="any_time_of_year">Any Time of Year</option>`;
	possibleSeason.forEach(z => {
		seasonFilter += `<option value="` + z + `" id="` + z + `">` + z + `</option>`
	})
	seasonFilter += `</select>`;

	return deptFilter + zoneFilter + timeOfDayFilter + seasonFilter;
}

var viewsHtml;
//to dynamically populate dropdown with validate options in hbase
hclient.table(hbaseTableName).scan({
			maxVersions: 1},
	(err, cells) => {
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
		viewsHtml = htmlViewTop + generateHtmlFilters() + htmlViewBottom;
});


function generateKeyFromRequest(req){
	//first extract args from request
	//and build key used to query hbase
	return 'cpdzone4mornspring';
}

//var viewsHtml = filesystem.readFileSync("views.html").toString();
/*
TO QUERY A PARTICULAR ROW
BASED OFF OF SELECTING FILTERS AND CLICKING SUBMIT
var key = generateKeyFromRequest(req);
	hclient.table(hbaseTableName).row(key).get((err, cells) => {
		console.log('aha !')
		console.log(cells);
		//do addtl processing here

		var html = mustache.render(viewsTemplate);
		res.send(html);

	})

*/

app.use(express.static('public'));
app.get('/', function (req, res) {
	//loads the main screen, so populate the template with filter options
	res.send(viewsHtml);
	
	//  const route=req.query['origin'] + req.query['dest'];
    // console.log(route);
	// hclient.table('weather_delays_by_route_v2').row(route).get(function (err, cells) {
	// 	const weatherInfo = rowToMap(cells);
	// 	console.log(weatherInfo)
	// 	function weather_delay(weather) {
	// 		var flights = weatherInfo["delay:" + weather + "_flights"];
	// 		var delays = weatherInfo["delay:" + weather + "_delays"];
	// 		if(flights == 0)
	// 			return " - ";
	// 		return (delays/flights).toFixed(1); /* One decimal place */
	// 	}
		
	// 	var template = filesystem.readFileSync("result.mustache").toString();
	// 	var html = mustache.render(template,  {
	// 		origin : req.query['origin'],
	// 		dest : req.query['dest'],
	// 		clear_dly : weather_delay("clear"),
	// 		fog_dly : weather_delay("fog"),
	// 		rain_dly : weather_delay("rain"),
	// 		snow_dly : weather_delay("snow"),
	// 		hail_dly : weather_delay("hail"),
	// 		thunder_dly : weather_delay("thunder"),
	// 		tornado_dly : weather_delay("tornado")
	// 	});
	// 	res.send(html);
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
	
	var date_ts = String(Date.now());
	//TODO: I should really just hash the input to gen the random id
	var id = "web_" + (date ? date : "_") + (time ? time : "_") + date_ts;
	var report = {
		id : id,
		dept_name : deptName,
		zone : zone,
		date : date,
		time : time,
		duration: duration,
		text : text,
		recording: null // TODO: I should really just make a separate flow and kafka topic if it needs to wait for audio processing
	};
	console.log(report);
	counter = counter + 1;
	if (req.file) {
		//upload audio file to s3
		var filepath = prefix + req.file.filename;
		var fileStream = fs.createReadStream(filepath);
		fileStream.on('error', function(err) {
			console.log('File Error', err);
		});		  
		var uploadParams = {Bucket: bucket, Key: '', Body: ''};
		uploadParams.Body = fileStream;
		//TODO: should just make a hash, maybe same as before, to get the key
		uploadParams.Key = "";
		if (deptName) uploadParams.Key += deptName;
		if (zone) uploadParams.Key += zone;
		if (date) uploadParams.Key += date;
		if (time) uploadParams.Key += time;
		uploadParams.Key += "." + date_ts + ".mp3";
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
