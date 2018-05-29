package kafka;

import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;


public class Publisher extends Thread {
	String topic;
	String url;
	
	public Publisher(String topic, String url) {
		this.topic = topic;
		this.url = url;
		new Publisher(topic, url).start();
	}
	

	@Override
	public void run() {
		Properties props = new Properties();
		
		//Localização dos servidores kafka (lista de máquinas + porto)
		props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		
		// Classe para serializar as chaves dos eventos (string)
		props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");
		
		// Classe para serializar os valores dos eventos (string)
		props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringSerializer");

		// Cria um novo editor (publisher), com as propriedades indicadas acima.
		
		Logger.getAnonymousLogger().log(Level.INFO, "Ligado ao Kafka; Dando inicio ao envio de eventos...");

		try (Producer<String, String> producer = new KafkaProducer<>(props)) {
			Random rand = new Random();
			while(true) {
				
				//publica eventos, cujo tópico (string) toma alternadamente o valor de: topic0 e topic1.
				producer.send(new ProducerRecord<String, String>("topic" + topic, "chave" + rand,
						"valor:" + url));
				sleep(5000);
			}
		}		
	}

	static void sleep(int ms) {
		try {
			Thread.sleep(ms);
		} catch (Exception x) {
		}
	}
	
	/*
	
	public static void main(String[] args) throws Exception {

		// Correr no docker o servidor kafka
		// 1) docker run -ti -p 2181:2181 -p 9092:9092 -p 2181:2181 smduarte/sd18-kafka

		
		//Desligar o logging do Kafka...
		//System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "ALL");
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "OFF");

		
		new Publisher(topic, url).start();
		
	}
	*/
}

