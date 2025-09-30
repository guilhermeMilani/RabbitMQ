package org.example.consumer;

import com.rabbitmq.client.AMQP;
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
                if (QUEUE_LUZ.equals(delivery.getEnvelope().getRoutingKey())
                        || msg.toLowerCase().contains("luz")) {
                    System.out.println("luz ligada com sucesso, mensagem recebida:" + msg);
                } else if (msg.toLowerCase().contains("ar")) {
                    System.out.println("ar ligado com sucesso, mensagem recebida:" + msg);
                }

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                int retryCount = delivery.getProperties().getHeaders() != null &&
                        delivery.getProperties().getHeaders().containsKey("x-retry-count")
                        ? (int) delivery.getProperties().getHeaders().get("x-retry-count") : 0;

                if (retryCount >= 5) {
                    System.out.println("A mensagem falhou mais de 5 vezes");
                    channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
                } else {
                    AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                            .headers(Map.of("x-retry-count", retryCount + 1))
                            .build();

                    channel.basicPublish("exchange.retry",
                            "retry." + delivery.getEnvelope().getRoutingKey(),
                            props,
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
