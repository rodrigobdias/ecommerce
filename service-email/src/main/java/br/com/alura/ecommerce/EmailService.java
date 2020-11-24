package br.com.alura.ecommerce;

import java.util.Map;
import java.util.concurrent.ExecutionException;

import br.com.alura.ecommerce.consumer.KafkaService;
import org.apache.kafka.clients.consumer.ConsumerRecord;

public class EmailService {

	public static void main(String[] args) throws ExecutionException, InterruptedException {
		var emailService = new EmailService();
		try (var service = new KafkaService(EmailService.class.getSimpleName(),
				"ECOMMERCE_SEND_EMAIL",
				emailService::parse,
				Map.of())) {
			service.run();
		}

	}

	private void parse(ConsumerRecord<String, Message<String>> record) {
		System.out.println("---------------------------------------------");
		System.out.println("Enviando email");
		System.out.println(record.key());
		System.out.println(record.value());
		System.out.println(record.partition());
		System.out.println(record.offset());
		try {
			Thread.sleep(1000);
		} catch (InterruptedException e) {
			// ignorar
			e.printStackTrace();
		}
		System.out.println("Email enviado");
	}

}
