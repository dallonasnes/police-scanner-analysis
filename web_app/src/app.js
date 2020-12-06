'use strict';
const http = require('http');
var assert = require('assert');
const express= require('express');
const app = express();
const mustache = require('mustache');
const filesystem = require('fs');
const url = require('url');
const port = Number(process.argv[2]);
var startYear = 2000;
const mostRecentYear = new Date().getFullYear() - 1; //-1 because we don't yet have data for a current year...although we'd have to update the data next year
var validYears = [];
while (startYear <= mostRecentYear){
	validYears.push(startYear);
	startYear++;
}

const hbase = require('hbase')
var client = hbase({ host: process.argv[3], port: Number(process.argv[4])})
/*
function rowToMap(row) {
	var stats = {}
	row.forEach(function (item) {
		stats[item['column']] = Number(item['$'])
	});
	return stats;
}


function weather_delay(flightInfoByYear, weather) {
	var totalFlights;
	var onTimeFlights;
	if (weather === "all"){
		totalFlights = flightInfoByYear["deps:all_departures"];
		onTimeFlights = flightInfoByYear["deps:all_on_time_departures"];
	} else {
		totalFlights = flightInfoByYear["deps:all_" + weather + "_departures"];
		onTimeFlights = flightInfoByYear["deps:" + weather + "_on_time_departures"];
	}
	if(totalFlights == 0)
		return " - ";
	return ((onTimeFlights/totalFlights)*100).toFixed(1) + "%";
}
*/
/*
async function getFlightDataByYear(airline){
	//the key of the hbase table is airline concatenated with year
	//since we want to display all the years, we iterate through all the years we know we have data for
	//and concat the year to the airline name in order to query for that row in the table


	//make all the years into promises
	var yearsPromises = [];
	validYears.forEach((year) => {
		var myPromise = new Promise((resolve, reject) => {
			var myAirline = airline + year;
			hclient.table('dasnes_hw61Final').row("zone*").get(async function (err, cells) {
				if (cells){
					const flightInfoByYear = rowToMap(cells);
					var flightInfoData = {
						Airline : airline,
						Year: year,
						All: weather_delay(flightInfoByYear, "all"),
						clear_dly : weather_delay(flightInfoByYear, "clear"),
						fog_dly : weather_delay(flightInfoByYear, "fog"),
						rain_dly : weather_delay(flightInfoByYear, "rain"),
						snow_dly : weather_delay(flightInfoByYear, "snow"),
						hail_dly : weather_delay(flightInfoByYear, "hail"),
						thunder_dly : weather_delay(flightInfoByYear, "thunder"),
						tornado_dly : weather_delay(flightInfoByYear, "tornado")
					};

				}
				//resolve(flightInfoData);

			});
		});
		yearsPromises.push(myPromise);
	});
	var flightDataByYear = [];
	await Promise.all(yearsPromises).then(values => flightDataByYear=values);
	//console.log("returning now")
	return flightDataByYear;
}
*/
app.use(express.static('public'));
app.get('/delays.html', function (req, res) {
	//const start = Date.now();
    /*const airline=req.query['Airline']
    var flightData = await getFlightDataByYear(airline);
    var flightDataDict = {};
    flightData.forEach((x) => flightDataDict[x.Year] = x);
	console.log("after flight data")
	var template = filesystem.readFileSync("result.mustache").toString();
	var html = mustache.render(template,  flightDataDict);*/
	console.log("hello world");
	client.table('dasnes_proj_csv_as_hbase').row("zone1*").get(function (err, cells) {
		if (cells) {
			console.log(cells);
		}
		res.send("hello world");
	})

	//console.log(Object.keys(flightData).length);
	//const end = Date.now();
	//console.log("Runtime: " + ((end - start)/1000));
});
	
app.listen(port);
