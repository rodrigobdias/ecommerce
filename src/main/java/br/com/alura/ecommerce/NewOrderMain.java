package br.com.alura.ecommerce;

import java.util.Properties;
import java.util.UUID;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {
		var producer = new KafkaProducer<String, String>(properties());

		for (var i = 0; i < 100; i++) {

			var key = UUID.randomUUID().toString();
			var value = key + ", 78754, 878752";
			var record = new ProducerRecord<String, String>("ECOMMERCE_NEW_ORDER", key, value);

			Callback callback = (data, ex) -> {
				if (ex != null) {
					ex.printStackTrace();
					return;
				}
				System.out.println("sucesso enviando " + data.topic() + ":::patition " + data.partition() + "/ offset "
						+ data.offset() + "/ timestamp " + data.timestamp());
			};

			producer.send(record, callback).get();

			var email = "Bem vindo, estamos processando sua ordem !";
			var emailRecord = new ProducerRecord<>("ECOMMERCE_SEND_EMAIL", key, email);

			producer.send(emailRecord, callback).get();
		}
	}

	private static Properties properties() {
		var properties = new Properties();
		properties.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
		properties.setProperty(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
		properties.setProperty(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

		return properties;
	}
}
