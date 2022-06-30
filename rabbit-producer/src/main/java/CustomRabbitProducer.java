import com.rabbitmq.client.ConnectionFactory;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.Channel;

import java.io.IOException;
import java.util.concurrent.TimeoutException;

public class CustomRabbitProducer {

    private final static String QUEUE_NAME = "hello";

    public static void main(String[] args) {
        ConnectionFactory factory = new ConnectionFactory();
        factory.setHost("rabbit");
        try (
                Connection connection = factory.newConnection();
                Channel channel = connection.createChannel()
        )
        {
            channel.queueDeclare(QUEUE_NAME, false, false, false, null);

            for(int i = 0; i < 10; i++)
                channel.basicPublish(
                        "",
                        QUEUE_NAME,
                        null,
                        msg(i)
                );

        } catch (IOException | TimeoutException e) {
            e.printStackTrace();
        }
    }

    private static byte[] msg(int i){
        return ("MESSAGE_" + i).getBytes();
    }

}
