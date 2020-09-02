package br.com.alura.ecommerce;

import java.util.Map;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

	public static void main(String[] args) {
		var fraudService = new FraudDetectorService();
		try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				fraudService::parse,
				Order.class,
				Map.of())) {
			service.run();
		}
	}

	private void parse(ConsumerRecord<String, Order> record) {
		System.out.println("---------------------------------------------");
		System.out.println("Processando nova ordem, detectando por fraude");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// ignorar
			e.printStackTrace();
		}
		System.out.println("Ordem processada");
	}
}
