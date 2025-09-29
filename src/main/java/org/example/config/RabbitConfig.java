package org.example.config;

import com.rabbitmq.client.BuiltinExchangeType;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

public class RabbitConfig {

    private static final String URI = "amqps://vzytdnvy:FdYQ9Xv7hPZqKI-lsQH1NwftJwBe1QJU@jaragua.lmq.cloudamqp.com/vzytdnvy";

    private static Connection connection;

    private RabbitConfig() {
    }

    public static synchronized Connection getConnection() throws Exception {
        if (connection == null || !connection.isOpen()) {
            ConnectionFactory factory = new ConnectionFactory();
            factory.setUri(URI);
            connection = factory.newConnection();
        }
        return connection;
    }

    public static void setupQueues(Channel channel) throws Exception {
        channel.exchangeDeclare("exchange.topic", BuiltinExchangeType.TOPIC, true);

        channel.queueDeclare("Fila.AcionamentosLuz", true, false, false, null);
        channel.queueDeclare("Fila.AcionamentosAr", true, false, false, null);
        channel.queueDeclare("Fila.Notificacao", true, false, false, null);

        channel.exchangeDeclare("exchange.retry", BuiltinExchangeType.TOPIC, true);
        channel.queueDeclare("Fila.Retry", true, false, false, null);

        channel.queueBind("Fila.AcionamentosLuz", "exchange.topic", "notificacao.luz");
        channel.queueBind("Fila.AcionamentosAr", "exchange.topic", "notificacao.ar");
        channel.queueBind("Fila.Notificacao", "exchange.topic", "notificacao.*");

        channel.queueBind("Fila.Retry", "exchange.retry", "retry.*");
    }
}

