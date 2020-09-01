package br.com.alura.ecommerce;

import java.util.UUID;
import java.util.concurrent.ExecutionException;

public class NewOrderMain {

	public static void main(String[] args) throws InterruptedException, ExecutionException {

		try (var dispatcher = new KafkaDispatcher()) {

			for (var i = 0; i < 10; i++) {

				var key = UUID.randomUUID().toString();

				var value = key + ", 78754, 878752";
				dispatcher.send("ECOMMERCE_NEW_ORDER", key, value);

				var email = "Bem vindo, estamos processando sua ordem !";
				dispatcher.send("ECOMMERCE_SEND_EMAIL", key, email);
			}
		}
	}
}
