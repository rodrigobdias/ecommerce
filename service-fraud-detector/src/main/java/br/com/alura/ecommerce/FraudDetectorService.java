package br.com.alura.ecommerce;

import java.math.BigDecimal;
import java.util.Map;
import java.util.concurrent.ExecutionException;

import org.apache.kafka.clients.consumer.ConsumerRecord;

public class FraudDetectorService {

	public static void main(String[] args) {
		var fraudService = new FraudDetectorService();
		try (var service = new KafkaService<>(FraudDetectorService.class.getSimpleName(), "ECOMMERCE_NEW_ORDER",
				fraudService::parse,
				Map.of())) {
			service.run();
		}
	}

	private final KafkaDispatcher<Order> orderDispatcher = new KafkaDispatcher<Order>();

	private void parse(ConsumerRecord<String, Message<Order>> record) throws ExecutionException, InterruptedException {
		System.out.println("---------------------------------------------");
		System.out.println("Processando nova ordem, detectando por fraude");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());

		var message = record.value();
		try {
			Thread.sleep(5000);
		} catch (InterruptedException e) {
			// ignorar
			e.printStackTrace();
		}

		var order = message.getPayload();
		if(isFraud(order)){
			// pretending that the fraud happens when the amount is >= 4500
			System.out.println("Ordem é uma fraude !!!!" + order);
			orderDispatcher.send("ECOMMERCE_ORDER_REJECTED", order.getEmail(),
					message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
					order);
		} else {
			System.out.println("Aprovado: " + order);
			orderDispatcher.send("ECOMMERCE_ORDER_APPROVED", order.getEmail(),
					message.getId().continueWith(FraudDetectorService.class.getSimpleName()),
					order);
		}


	}

	private boolean isFraud(Order order) {
		return order.getAmount().compareTo(new BigDecimal("4500")) >= 0;
	}
}
