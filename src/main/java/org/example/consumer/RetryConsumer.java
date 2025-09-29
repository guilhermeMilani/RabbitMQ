package org.example.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.example.config.RabbitConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class RetryConsumer {
    private static final Logger logger = LoggerFactory.getLogger(RetryConsumer.class);

    private static final String RETRY_QUEUE = "Fila.Retry";
    private static final String MAIN_EXCHANGE = "exchange.topic";

    public static void main(String[] args) throws Exception {
        Connection connection = RabbitConfig.getConnection();
        Channel channel = connection.createChannel();

        DeliverCallback callback = (consumerTag, delivery) -> {
            String msg = new String(delivery.getBody(), "UTF-8");
            Map<String, Object> headers = delivery.getProperties().getHeaders();

            int retryCount = headers != null && headers.containsKey("x-retry-count")
                    ? (int) headers.get("x-retry-count")
                    : 0;

            if (retryCount >= 5) {
                logger.error("mensagem falhou após 5 tentativas: {}", msg);
                channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
                return;
            }

            try {
                Thread.sleep(10_000);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }

            logger.warn("reenviando mensagem, tentativa {}: {}", retryCount + 1, msg);

            AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                    .headers(Map.of("x-retry-count", retryCount + 1))
                    .build();

            channel.basicPublish(MAIN_EXCHANGE,
                    delivery.getEnvelope().getRoutingKey(),
                    props,
                    delivery.getBody());

            channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
        };

        channel.basicConsume(RETRY_QUEUE, false, callback, consumerTag -> {
        });
        logger.info("retry iniciado, monitorando fila: {}", RETRY_QUEUE);
    }
}
