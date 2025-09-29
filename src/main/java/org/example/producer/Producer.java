package org.example.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.example.config.RabbitConfig;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class Producer {
    private static final Logger logger = LoggerFactory.getLogger(Producer.class);

    public static void main(String[] args) throws Exception {
        try (Connection connection = RabbitConfig.getConnection();
             Channel channel = connection.createChannel()) {

            RabbitConfig.setupQueues(channel);

            String message = "ligar Luz";
            channel.basicPublish("exchange.topic", "notificacao.luz", null, message.getBytes());
            logger.info("mensagem enviada: {}", message);

            String message2 = "ligar Ar - temperatura 22ºC";
            channel.basicPublish("exchange.topic", "notificacao.ar", null, message2.getBytes());
            logger.info("mensagem enviada: {}", message2);
        }
    }
}

