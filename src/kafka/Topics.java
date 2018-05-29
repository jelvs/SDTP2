package kafka;

import java.util.Properties;

import org.I0Itec.zkclient.ZkClient;
import org.I0Itec.zkclient.ZkConnection;
import org.apache.kafka.common.errors.TopicExistsException;

import kafka.admin.AdminUtils;
import kafka.utils.*;
import kafka.utils.ZKStringSerializer;
import kafka.utils.ZKStringSerializer$;
import kafka.utils.ZkUtils;

public class Topics {

	private static final int SESSION_TIMEOUT = 5000;
	private static final int CONNECTION_TIMEOUT = 1000;
	private static final String ZOOKEEPER_SERVER = "localhost:2181";

	private static final int REPLICATION_FACTOR = 1;

	
	public static void createTopic( String topic, int numPartitions ) {
		try {
			ZkClient zkClient = new ZkClient(
		            ZOOKEEPER_SERVER,
		            SESSION_TIMEOUT,
		            CONNECTION_TIMEOUT,
		            ZKStringSerializer$.MODULE$);
			
			ZkUtils zkUtils = new ZkUtils(zkClient, new ZkConnection(ZOOKEEPER_SERVER), false);
	
			
			Properties topicConfig = new Properties();
			AdminUtils.createTopic(zkUtils, topic, numPartitions, REPLICATION_FACTOR, topicConfig, null);		
		} catch( TopicExistsException e ) {		
			System.err.println("Topic already exists...");
		}
	}
	/*
	public static void main(String[] args ) throws Exception {
		// Correr no docker o servidor kafka
		// 1) docker run -ti -p 2181:2181 -p 9092:9092 -p 2181:2181 smduarte/kafka

	
		//Desligar o logging do Kafka...
		//System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "ALL");
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "OFF");


		int numPartitions = 1;
		String topic = "sd2018-tp1";
		createTopic(topic, numPartitions);
	}
	*/
}
