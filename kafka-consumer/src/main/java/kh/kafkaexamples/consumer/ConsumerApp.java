package kh.kafkaexamples.consumer;

import java.io.IOException;
import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

public class ConsumerApp {

	private Properties props;

	public static void main(String[] args) throws Exception {
		ConsumerApp app = new ConsumerApp();
		app.init();
		app.consume();
	}

	public void init() throws IOException {
		this.props = new Properties();
		props.load(this.getClass().getResourceAsStream("/consumer.properties"));
	}

	public void consume() throws InterruptedException {
		KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
	     consumer.subscribe(Arrays.asList("test-topic"));
	     while (true) {
	         ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
	         for (ConsumerRecord<String, String> record : records)
	             System.out.printf("offset = %d, key = %s, value = %s%n", record.offset(), record.key(), record.value());
	     }
	}

}
