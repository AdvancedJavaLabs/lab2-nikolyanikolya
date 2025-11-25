import com.rabbitmq.client.ConnectionFactory;

public class MqConfigProvider {

  ConnectionFactory connectionFactory() {
    ConnectionFactory factory = new ConnectionFactory();
    factory.setHost("localhost");
    factory.setPort(5672);
    factory.setUsername("guest");
    factory.setPassword("guest");
    return factory;
  }
}
