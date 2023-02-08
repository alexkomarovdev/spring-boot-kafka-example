package com.esoft.kafka;

import com.esoft.kafka.consumer.Listener;
import com.esoft.kafka.producer.Producer;
import org.apache.kafka.clients.producer.internals.Sender;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

@SpringBootApplication
public class KafkaApplication  implements CommandLineRunner{

	public static void main(String[] args) {
		SpringApplication.run(KafkaApplication.class, args);
	}

    @Value("${app.topic}")
    private String topicName;    

    @Autowired
    private Producer producer;

    @Override
    public void run(String... strings) throws Exception {
        for (int i = 0; i < 3; i++){
            this.producer.sendMessage(this.topicName, String.valueOf(i), "Сообщение " + i + " для " + this.topicName);
        }
		System.out.println("Сообщения отправлены");
    }
}