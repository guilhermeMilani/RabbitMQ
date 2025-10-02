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

                while (!"notificacao.ar".equalsIgnoreCase(routingKey)
                        && !"notificacao.luz".equalsIgnoreCase(routingKey)
                        && !"falha.luz".equalsIgnoreCase(routingKey)
                        && !"falha.ar".equalsIgnoreCase(routingKey)) {

                    System.out.print("Digitação errada, tente novamente ou saia: ");
                    routingKey = scanner.nextLine();

                    if ("sair".equalsIgnoreCase(routingKey)) break;
                }

                if ("sair".equalsIgnoreCase(routingKey)) break;

                String message = "";

                if (routingKey.toLowerCase().startsWith("falha")) {
                    message = "falha";
                }else {
                    while (true) {
                        System.out.print("digite a ação (ligar/desligar): ");
                        message = scanner.nextLine().toLowerCase();
                        if ("ligar".equals(message) || "desligar".equals(message)) {
                            break;
                        } else {
                            System.out.println("Ação inválida! Só pode 'ligar' ou 'desligar'. Tente de novo.");
                        }
                    }
                }
                channel.basicPublish("exchange.topic", routingKey, null, message.getBytes());
                System.out.println("chave: " + routingKey + " mensagem: " + message);
            }
        }
    }
}

