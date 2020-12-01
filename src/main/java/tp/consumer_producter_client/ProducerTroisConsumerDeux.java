package tp.consumer_producter_client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import tp.database.Database;

import java.time.Duration;
import java.util.Collections;
import java.util.Scanner;

public class ProducerTroisConsumerDeux implements Runnable {

    private Producer<String, String> producer;
    private Consumer<String, String> consumer;
    private String topicProducer = "Topic3";
    private String topicConsumer = "Topic2";
    private Database database;

    public ProducerTroisConsumerDeux() {
        producer = ProducerFactory.createProducer();
        consumer = ConsumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton(topicConsumer));
        database = Database.getInstance();
    }

    @Override
    public synchronized void run() {
        while (!Thread.interrupted()){
            //On récupère la command du topic 2
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(100));
            records.forEach(stringStringConsumerRecord -> {
                String message = stringStringConsumerRecord.value();
                String[] message_split = message.split(" ");
                String result;
                switch (message_split[0]){
                    case "get_global_values":
                        result = database.getGlobal();
                        break;
                    case "get_country_values":
                        result = database.getCountry(message_split[1].toUpperCase());
                        break;
                    case "get_confirmed_avg":
                        result = database.getAvgConfirmed();
                        break;
                    case "get_deaths_avg":
                        result = database.getAvgDeaths();
                        break;
                    case "get_countries_deaths_percent":
                        result = database.getPercentDeaths();
                        break;
                    default:
                        result = "ERRORS:Command not found";
                        break;
                }
                //envoie du résultat sur le topic 3
                ProducerRecord<String, String> record = new ProducerRecord<String, String>(topicProducer, result);
                producer.send(record, new ProducerCallBack());
            });
        }
        consumer.close();
        producer.close();
    }

    private class ProducerCallBack implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null){
                e.printStackTrace();
            }
        }
    }


}
