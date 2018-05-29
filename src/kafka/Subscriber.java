package kafka;

import java.util.Arrays;
import java.util.Collection;
import java.util.Properties;
import java.util.Random;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class Subscriber extends Thread {

	private static final long TIMEOUT = 1000;
	private static Collection<String> Topics;
	ConsumerRecords<String,String> finalRecords;
	
	public Subscriber(Collection<String> Topics) {
		this.Topics = Topics;
		new Subscriber(Topics).start();
	}
	
	public ConsumerRecords<String, String> info() {
		
		return finalRecords;
		
	}
	@Override
	public void run() {
		
		Random rnd = new Random();
		
		Properties props = new Properties();

		//Localização dos servidores kafka (lista de máquinas + porto)
		props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		
		//Configura o modo de subscrição (ver documentação em kafka.apache.org)
		props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
		props.put(ConsumerConfig.GROUP_ID_CONFIG, "grp" + rnd.nextLong());
		
		// Classe para serializar as chaves dos eventos (string)
		props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		
		// Classe para serializar os valores dos eventos (string)
		props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, "org.apache.kafka.common.serialization.StringDeserializer");
		
		
		// Cria um consumidor (assinante/subscriber)
		try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props)) {
			// Subscreve uma lista de tópicos
			
			consumer.subscribe(Topics);
			
			
			Logger.getAnonymousLogger().log(Level.INFO, "Ligado ao Kafka; Esperando por eventos...");
			//ciclo para receber os eventos
			while (true) {
				
				//pede ao servidor uma lista de eventos, até esgotar o TIMEOUT
				ConsumerRecords<String, String> records = consumer.poll(TIMEOUT);
				finalRecords = records;
				info();
				
				
				records.forEach(r -> {
					//Apresenta o evento, incluindo o seu tópico e o seu "offset" na partição kafka onde foi escrito.
					//Para um dado tópico, havendo uma só partição (configuração default), o offset é um valor inteiro (long) 
					//totalmente ordenado na perspectiva de todos os assinantes do tópico.
					System.out.printf("topic = %s, key = %s, value = %s, offset=%s%n", r.topic(), r.key(), r.value(), r.offset());
				});
			}
		} 
	}

	/*
	public static void main(String[] args) throws Exception {

		// Correr no docker o servidor kafka
		// 1) docker run -ti -p 9092:9092 smduarte/kafka

		//Desligar o logging do Kafka...
		//System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "ALL");
		System.setProperty("org.slf4j.simpleLogger.defaultLogLevel", "OFF");

		
		new Subscriber().start();
	}
	*/
}
