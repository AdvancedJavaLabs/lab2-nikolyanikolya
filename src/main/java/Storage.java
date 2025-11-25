import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.io.*;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

public class Storage {
  ObjectMapper mapper = new ObjectMapper();

  public void save(String dirName, String body) {
    try {
      mapper.readTree(body);
      LocalDateTime now = LocalDateTime.now();
      DateTimeFormatter formatter = DateTimeFormatter.ofPattern("yyyy-MM-dd_HH-mm-ss");
      String timestamp = now.format(formatter);
      File file = new File(String.format("%s/%s.json", dirName, timestamp));
      try (FileWriter out = new FileWriter(file)) {
        out.write(body);
        out.flush();
      } catch (IOException e) {
        throw new RuntimeException(e);
      }
    } catch (JsonProcessingException e) {
      System.out.println("Invalid json");
      throw new RuntimeException(e);
    }
  }
}
