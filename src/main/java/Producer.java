import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;
import java.nio.charset.StandardCharsets;

public class Producer {
  private static final String EXCHANGE = "workers_exchange";
  private static final String ROUTING_KEY = "task";

  public static void main(String[] args) throws Exception {
    var configProvider = new MqConfigProvider();
    ConnectionFactory factory = configProvider.connectionFactory();

    try (Connection conn = factory.newConnection();
         Channel ch = conn.createChannel()) {

      ch.exchangeDeclare(EXCHANGE, "direct", true);

      String message = "{\"task\":\"process_this\",\"id\":123}";
      AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
        .deliveryMode(2) // persistent
        .contentType("application/json")
        .build();

      ch.basicPublish(EXCHANGE, ROUTING_KEY, props, message.getBytes(StandardCharsets.UTF_8));
      System.out.println("Sent: " + message);
    }
  }
}

