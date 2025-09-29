package org.example.consumer;

import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.DeliverCallback;
import org.example.config.RabbitConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Map;

public class Consumer {
    private static final Logger logger = LoggerFactory.getLogger(Consumer.class);
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
                    logger.info("luz ligada com sucesso, Mensagem recebida: {}", msg);
                } else if (msg.toLowerCase().contains("ar")) {
                    logger.info("ar condicionado ligado com sucesso, Mensagem recebida: {}", msg);
                }

                channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
            } catch (Exception e) {
                logger.error("Erro ao processar mensagem: {}", msg, e);

                int retryCount = delivery.getProperties().getHeaders() != null &&
                        delivery.getProperties().getHeaders().containsKey("x-retry-count")
                        ? (int) delivery.getProperties().getHeaders().get("x-retry-count") : 0;

                if (retryCount >= 5) {
                    logger.error("a requisição falhou após 5 tentativas: {}", msg);
                    channel.basicReject(delivery.getEnvelope().getDeliveryTag(), false);
                } else {
                    AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
                            .headers(Map.of("x-retry-count", retryCount + 1))
                            .build();

                    channel.basicPublish("exchange.retry", "retry." + delivery.getEnvelope().getRoutingKey(),
                            props, delivery.getBody());
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
