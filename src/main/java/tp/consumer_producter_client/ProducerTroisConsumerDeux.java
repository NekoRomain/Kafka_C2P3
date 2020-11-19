package tp.consumer_producter_client;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Collections;
import java.util.Scanner;

public class ProducerTroisConsumerDeux implements Runnable {

    private Producer<String, String> producer;
    private Consumer<String, String> consumer;
    private String topicProducer = "Topic3";
    private String topicConsumer = "Topic2";
    private Scanner scanner;

    public ProducerTroisConsumerDeux() {
        producer = ProducerFactory.createProducer();
        consumer = ConsumerFactory.createConsumer();
        consumer.subscribe(Collections.singleton(topicConsumer));
    }

    @Override
    public synchronized void run() {

        while (!Thread.interrupted()){

        }
        scanner.close();
    }

    private class ProducerCallBack implements Callback {
        @Override
        public void onCompletion(RecordMetadata recordMetadata, Exception e) {
            if (e != null){
                e.printStackTrace();
            }
        }
    }

    private String sqlCommand(String cmd){
        String[] cmd_split = cmd.split(" ");
        switch (cmd_split[0]){
            case "Get_global_values":
                break;
            case "Get_country_values":
                break;
            case "Get_confirmed_avg":

        }
        return "";
    }

}
