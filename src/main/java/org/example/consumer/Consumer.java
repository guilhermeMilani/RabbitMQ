package org.example.consumer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.example.config.RabbitConfig;

import java.util.Map;

public class Consumer {
    private static final String QUEUE_LUZ = "Fila.AcionamentosLuz";
    private static final String QUEUE_AR = "Fila.AcionamentosAr";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitConfig.getConnection();
        Channel channel = connection.createChannel();

        DeliverCallback callback = (consumerTag, delivery) -> {
            String msg = new String(delivery.getBody(), "UTF-8");
            try {
                if ("falha.ar".equals(delivery.getEnvelope().getRoutingKey())
                        || "falha.luz".equals(delivery.getEnvelope().getRoutingKey())) {
                    throw new RuntimeException("Mensagem forçada para falhar");
                }
                if ("notificacao.luz".equals(delivery.getEnvelope().getRoutingKey())) {
                    if ("ligar".equals(msg)) {
                        System.out.println("Luz ligada com sucesso!");
                    } else if ("desligar".equals(msg)) {
                        System.out.println("Luz desligada com sucesso!");
                    } else {
                        System.out.println("⚠Mensagem inválida para luz: " + msg);
                    }

                } else if ("notificacao.ar".equals(delivery.getEnvelope().getRoutingKey())) {
                    if ("ligar".equals(msg)) {
                        System.out.println("❄Ar-condicionado ligado com sucesso!");
                    } else if ("desligar".equals(msg)) {
                        System.out.println("❄Ar-condicionado desligado com sucesso!");
                    } else {
                        System.out.println("⚠Mensagem inválida para ar-condicionado: " + msg);
                    }
                }

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                Map<String, Object> headers = delivery.getProperties().getHeaders();
                int retryCount = 0;
                if (headers != null && headers.get("x-retry-count") != null) {
                    retryCount = (int) headers.get("x-retry-count");
                }

                if (retryCount >= 5) {
                    System.out.println("A mensagem falhou mais de 5 vezes");
                    channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
                } else {
                    Map<String, Object> newHeaders = Map.of("x-retry-count", retryCount + 1);
                    channel.basicPublish("exchange.retry",
                            "retry." + delivery.getEnvelope().getRoutingKey(),
                            new com.rabbitmq.client.AMQP.BasicProperties.Builder()
                                    .headers(newHeaders)
                                    .build(),
                            delivery.getBody());

                    channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
                }
            }
         };

        channel.basicConsume(QUEUE_LUZ, false, callback, consumerTag -> {
        });
        channel.basicConsume(QUEUE_AR, false, callback, consumerTag -> {
        });
    }
}
