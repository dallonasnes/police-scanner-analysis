'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);

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

app.get('/weather.html',function (req, res) {
	// var station_val = req.query['station'];
	// var fog_val = (req.query['fog']) ? true : false;
	// var rain_val = (req.query['rain']) ? true : false;
	// var snow_val = (req.query['snow']) ? true : false;
	// var hail_val = (req.query['hail']) ? true : false;
	// var thunder_val = (req.query['thunder']) ? true : false;
	// var tornado_val = (req.query['tornado']) ? true : false;
	var report = {
		zone_timestamp : "fromclient" + counter,
		text : "hello world"
	};
	counter = counter + 1;
	kafkaProducer.send([{ topic: 'topic_dasnes_transcription_finished', messages: JSON.stringify(report)}],
		function (err, data) {
			console.log("and made it back in here")
			console.log("Kafka Error: " + err)
			console.log(data);
			console.log(report);
			res.redirect('submit-weather.html');
		});
});

var report = {
	zone_timestamp : "fromclient",
	text : "hello world"
};
kafkaProducer.send([{ topic: 'topic_dasnes_transcription_finished', messages: JSON.stringify(report)}],
	function (err, data) {
		console.log("Kafka Error: " + err)
		console.log(data);
		console.log(report);
	});

app.listen(port);
