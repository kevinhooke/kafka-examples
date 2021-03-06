package kh.kafkaexamples.producer;

import java.io.IOException;
import java.util.Properties;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

public class ProducerApp {

	private Properties props;

	public static void main(String[] args) throws Exception {
		ProducerApp app = new ProducerApp();
		app.init();
		app.produce();
	}

	public void init() throws IOException {
		this.props = new Properties();
		props.load(this.getClass().getResourceAsStream("/producer.properties"));
	}

	public void produce() throws InterruptedException {
		Producer<String, String> producer = new KafkaProducer<>(props);
		producer.send(new ProducerRecord<String, String>("test-topic", "1", "test message"));
		producer.flush();
		producer.close();
	}

}
