package rabbitMQ.example.demorabbitMQ;
import com.rabbitmq.client.*;

import java.nio.charset.StandardCharsets;

public class Recv {
    private final static String QUEUE_NAME = "hello";

    private static int fib(int n){
        if (n==0) return 0;
        if (n==1) return 1;
        return fib(n-1) + fib(n-2);
    }

    public static void main(String[] argv) throws Exception{
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("localhost");
        Connection connection = factory.newConnection();
        Channel channel = connection.createChannel();


        channel.queueDeclare(QUEUE_NAME, false, false, false, null);
        channel.queuePurge(QUEUE_NAME);

        channel.basicQos(1);

        System.out.println("Waiting for messages");

        DeliverCallback deliverCallback = (consumerTag, delivery) -> {
            AMQP.BasicProperties replyProps = new AMQP.BasicProperties.Builder().correlationId(delivery.getProperties().getCorrelationId()).build();

            String response = "";
             try {
                 String message = new String(delivery.getBody(),"UTF-8");
                 int n = Integer.parseInt(message);

                 System.out.println(" [.] fib(" + message + ")");
                 response += fib(n);
             }
             catch(RuntimeException e){
                 System.out.println("[.]" + e);
             }
            finally {
                 channel.basicPublish("", delivery.getProperties().getReplyTo(), replyProps, response.getBytes("UTF-8"));
                 channel.basicAck(delivery.getEnvelope().getDeliveryTag(), false);
             }

        };
        channel.basicConsume(QUEUE_NAME, true, deliverCallback, consumerTag->{});
    }
}
