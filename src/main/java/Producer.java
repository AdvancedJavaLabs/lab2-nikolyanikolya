import com.fasterxml.jackson.databind.ObjectMapper;
import com.rabbitmq.client.AMQP;
import com.rabbitmq.client.Channel;
import com.rabbitmq.client.Connection;
import com.rabbitmq.client.ConnectionFactory;

import java.nio.charset.StandardCharsets;
import java.util.concurrent.atomic.AtomicLong;

public class Producer {
  private static final String EXCHANGE = "workers_exchange";
  private static final String ROUTING_KEY = "task";
  private static final AtomicLong taskIdGenerator = new AtomicLong(0);
  private static final ObjectMapper mapper = new ObjectMapper();

  public static Long submit(String block) throws Exception {
    var configProvider = new MqConfigProvider();
    ConnectionFactory factory = configProvider.connectionFactory();
    try (Connection conn = factory.newConnection();
         Channel ch = conn.createChannel()) {

      ch.exchangeDeclare(EXCHANGE, "direct", true);

      var taskId = taskIdGenerator.getAndIncrement();
      Task task = new Task(taskId, block);
      String message = mapper.writeValueAsString(task);
      AMQP.BasicProperties props = new AMQP.BasicProperties.Builder()
        .deliveryMode(2)
        .contentType("application/json")
        .build();

      System.out.printf("Task %s with size %s was sent by Producer (thread = %s)\n", taskId, block.length(), Thread.currentThread().getName());

      ch.basicPublish(EXCHANGE, ROUTING_KEY, props, message.getBytes(StandardCharsets.UTF_8));
      return taskId;
    }
  }

}

