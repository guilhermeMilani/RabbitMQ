package org.example.producer;

import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import org.example.config.RabbitConfig;

import java.util.Scanner;

public class Producer {
    public static void main(String[] args) throws Exception {
        try (Connection connection = RabbitConfig.getConnection();
             Channel channel = connection.createChannel();
             Scanner scanner = new Scanner(System.in)) {

            RabbitConfig.setupQueues(channel);

            while (true) {
                System.out.print("digite a chave (ex: notificacao.luz ou notificacao.ar) ou 'sair' para sair: ");
                String routingKey = scanner.nextLine();
                if ("sair".equalsIgnoreCase(routingKey)) break;

                System.out.print("digite a mensagem: ");
                String message = scanner.nextLine();

                channel.basicPublish("exchange.topic", routingKey, null, message.getBytes());
                System.out.println("chave: " + routingKey + " mensagem: " + message);
            }
        }
    }
}

