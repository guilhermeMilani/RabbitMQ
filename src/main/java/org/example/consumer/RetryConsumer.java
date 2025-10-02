package org.example.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.example.config.RabbitConfig;

import java.util.Map;

public class RetryConsumer {

    private static final String RETRY_QUEUE = "Fila.Retry";
    private static final String MAIN_EXCHANGE = "exchange.topic";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitConfig.getConnection();
        Channel channel = connection.createChannel();

        DeliverCallback callback = (consumerTag, delivery) -> {
            String msg = new String(delivery.getBody(),"UTF-8").toLowerCase();
            Map<String, Object> headers = delivery.getProperties().getHeaders();

            int retryCount = headers != null && headers.containsKey("x-retry-count")
                    ? (int) headers.get("x-retry-count")
                    : 0;

            if (retryCount >= 5) {
                System.out.println("mensagem falhou após 5 tentativas");
                channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
                return;
            }

            String routingKey = delivery.getEnvelope().getRoutingKey();


            if ("retry.falha.ar".equals(routingKey)) {
                msg = "ligar";
                routingKey = "notificacao.ar";
                System.out.println("Falha AR convertida para comando real: " + msg);
            } else if ("retry.falha.luz".equals(routingKey)) {
                msg = "ligar";
                routingKey = "notificacao.luz";
                System.out.println("Falha LUZ convertida para comando real: " + msg);
            }

            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .headers(Map.of("x-retry-count", retryCount + 1))
                    .build();

            channel.basicPublish(MAIN_EXCHANGE,
                    routingKey,
                    props,
                    msg.getBytes());

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        channel.basicConsume(RETRY_QUEUE, false, callback, consumerTag -> {
        });
    }
}
