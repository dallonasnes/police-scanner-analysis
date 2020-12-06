package org.example;
import java.io.*;
import java.net.*;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;
import java.util.Random;

import javax.ws.rs.client.Client;
import javax.ws.rs.client.ClientBuilder;
import javax.ws.rs.client.Invocation;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.glassfish.jersey.jackson.JacksonFeature;

import com.amazonaws.AmazonServiceException;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.example.FleetArrivedResponse.FleetArrivedResult.Arrival;
import org.example.FlightInfoExResponse.FlightInfoExResult.Flight;
import org.example.Secrets;

// Inspired by http://stackoverflow.com/questions/14458450/what-to-use-instead-of-org-jboss-resteasy-client-clientrequest
public class FleetArrivals {
	static class Task extends TimerTask {
		private Client client;
		Random generator = new Random();
		// We are just going to get a random sampling of flights from a few airlines
		// Getting all flights would be much more expensive!
		String[] airlines = new String[]{"DAL", "AAL", "SWA", "UAL"};

		public FleetArrivedResponse getFleetArrivedResponse() {
			Invocation.Builder bldr
					= client.target("http://flightxml.flightaware.com/json/FlightXML2/FleetArrived?fleet="
					+ airlines[generator.nextInt(airlines.length)]
					+ "&howMany=3&offset=0").request("application/json");
			try {
				return bldr.get(FleetArrivedResponse.class);
			} catch (Exception e) {
				System.err.println(e.getMessage());
			}
			return null;  // Sometimes the web service fails due to network problems. Just let it try again
		}

		public FlightInfoExResponse getFlightInfoExResponse(String ident, int departureTime) {
			String uri = String.format("http://flightxml.flightaware.com/json/FlightXML2/FlightInfoEx?ident=%s@%d&howMany=1&offset=0", ident, departureTime);
			Invocation.Builder bldr = client.target(uri).request("application/json");
			return bldr.get(FlightInfoExResponse.class);

		}

		// Adapted from http://hortonworks.com/hadoop-tutorial/simulating-transporting-realtime-events-stream-apache-kafka/
		Properties props = new Properties();
		String TOPIC = "dasnes-scanner-audio-uri";
		KafkaProducer<String, String> producer;
		ProcessBuilder processBuilder = new ProcessBuilder();


		public Task() {
			client = ClientBuilder.newClient();
			// enable POJO mapping using Jackson - see
			// https://jersey.java.net/documentation/latest/user-guide.html#json.jackson
			client.register(JacksonFeature.class);
			props.put("bootstrap.servers", bootstrapServers);
			props.put("acks", "all");
			props.put("retries", 0);
			props.put("batch.size", 16384);
			props.put("linger.ms", 1);
			props.put("buffer.memory", 33554432);
			props.put("key.serializer", "org.apache.kafka.common.serialization.StringSerializer");
			props.put("value.serializer", "org.apache.kafka.common.serialization.StringSerializer");

			producer = new KafkaProducer<>(props);
		}

		@Override
		public void run() {
			//this attempt inspired by: https://stackoverflow.com/questions/46073200/record-file-audio-from-url-using-java
			try {


				Secrets secrets = new Secrets();
				String username = secrets.getUn();
				String password = secrets.getPw();

				long t = System.currentTimeMillis();
				String filename = String.format("/home/hadoop/dasnes/kafka/target/testFile1.mp3", t);

				Regions clientRegion = Regions.DEFAULT_REGION;
				String bucketName = "dasnes-mpcs53014";
				String fileObjKeyName = "test1.mp3";
				AmazonS3Client s3Client = new AmazonS3Client();

				processBuilder.command("bash", "-c", "wget --http-user=" + username + " --http-password=" + password + " https://audio.broadcastify.com/27730.mp3 > " + filename);
				Process process = processBuilder.start();

				while (System.currentTimeMillis() - t <= 20 * 1000) {
					//do nothing
				}
				//then kill the process and upload the file to S3
				process.destroy();

				PutObjectRequest request = new PutObjectRequest(bucketName, fileObjKeyName, new File(filename));
				ObjectMetadata metadata = new ObjectMetadata();
				metadata.setContentType("plain/text");
				metadata.addUserMetadata("title", "someTitle");
				request.setMetadata(metadata);
				s3Client.putObject(request);

				producer.send(new ProducerRecord<String, String>(TOPIC, "hello world from dallon"));

			} catch (Exception e) {
				e.printStackTrace();
			}
			/*
			FleetArrivedResponse response = getFleetArrivedResponse();
			if(response == null || response.getFleetArrivedResult() == null)
				return;
			ObjectMapper mapper = new ObjectMapper();

			for(Arrival arrival : response.getFleetArrivedResult().getArrivals()) {
				ProducerRecord<String, String> data;
				try {
					Flight f
					  = getFlightInfoExResponse(arrival.getIdent(), arrival.getActualdeparturetime()).getFlightInfoExResult().getFlights()[0];
					KafkaFlightRecord kfr = new KafkaFlightRecord(
							f.getIdent(),
							f.getOrigin().substring(1), 
							f.getDestination().substring(1), 
							(f.getActualdeparturetime() - f.getFiledDeparturetime())/60);
					data = new ProducerRecord<String, String>
					(TOPIC, 
					 mapper.writeValueAsString(kfr));
					producer.send(data);
				} catch (JsonProcessingException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}*/
		}
	}





	static String bootstrapServers = new String("localhost:9092");

	public static void main(String[] args) {
		if(args.length > 0)  // This lets us run on the cluster with a different kafka
			bootstrapServers = args[0];
		Timer timer = new Timer();
		timer.scheduleAtFixedRate(new Task(), 0, 20*1000);
	}
}

