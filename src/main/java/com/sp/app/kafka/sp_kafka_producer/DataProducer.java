package com.sp.app.kafka.sp_kafka_producer;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Properties;
import java.util.Random;
import java.util.UUID;

import org.apache.log4j.Logger;

import com.sp.app.kafka.util.PropertyFileReader; //Helper class to load properties files
import com.sp.app.kafka.vo.SPData; //Utility class used for serialization

import kafka.javaapi.producer.Producer;
import kafka.producer.KeyedMessage;
import kafka.producer.ProducerConfig;

/**
 * Hello world!
 *
 */
public class DataProducer {
	private static final Logger logger = Logger.getLogger(DataProducer.class);

	public static void main(String[] args) throws Exception {

		// read configuration file
		Properties prop = PropertyFileReader.readPropertyFile();
		String zookeeper = prop.getProperty("com.sp.app.kafka.zookeeper");
		String brokerList = prop.getProperty("com.sp.app.kafka.brokerlist");
		String topic = prop.getProperty("com.sp.app.kafka.topic");
		logger.info("Using Zookeeper=" + zookeeper + " ,Broker-list=" + brokerList + " and topic " + topic);

		// set producer properties
		Properties properties = new Properties();
		properties.put("zookeeper.connect", zookeeper);
		properties.put("metadata.broker.list", brokerList);
		properties.put("request.required.acks", "1");
		properties.put("serializer.class", "com.sp.app.kafka.util.SPDataEncoder");

		// generate event
		Producer<String, SPData> producer = new Producer<String, SPData>(new ProducerConfig(properties));
		DataProducer SPProducer = new DataProducer();
		SPProducer.generateSPEvent(producer, topic);

	}

	/**
	 * Method runs in while loop and generates random data in JSON with below
	 * format.
	 * 
	 * 
	 * @throws InterruptedException
	 * 
	 * 
	 */
	private void generateSPEvent(Producer<String, SPData> producer, String topic) throws InterruptedException {
		List<String> cityList = Arrays.asList(new String[] { "Route-37", "Route-43", "Route-82" });
		List<String> vehicleTypeList = Arrays
				.asList(new String[] { "Large Truck", "Small Truck", "Private Car", "Bus", "Taxi" });
		Random rand = new Random();
		logger.info("Sending events");
		// generate event in loop
		while (true) {
			List<SPData> eventList = new ArrayList<SPData>();
			for (int i = 0; i < 100; i++) {// create 100 vehicles
				String vehicleId = UUID.randomUUID().toString();
				String vehicleType = vehicleTypeList.get(rand.nextInt(5));
				String routeId = cityList.get(rand.nextInt(3));
				Date timestamp = new Date();
				double speed = rand.nextInt(100 - 20) + 20;// random speed between 20 to 100
				for (int j = 0; j < 5; j++) {// Add 5 events for each vehicle

					SPData event = new SPData(vehicleId, (float) speed, vehicleType, routeId, timestamp);
					eventList.add(event);
				}
			}
			Collections.shuffle(eventList);// shuffle for random events
			for (SPData event : eventList) {
				KeyedMessage<String, SPData> data = new KeyedMessage<String, SPData>(topic, event);
				producer.send(data);
				Thread.sleep(rand.nextInt(3000 - 1000) + 1000);// random delay of 1 to 3 seconds
			}
		}
	}
}
