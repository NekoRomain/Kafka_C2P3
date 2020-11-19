package tp;

import tp.consumer_producter_client.ProducerTroisConsumerDeux;

public class Main{

        public Main() throws InterruptedException {
            ProducerTroisConsumerDeux c2p3 = new ProducerTroisConsumerDeux();

            Thread thread = new Thread(c2p3);
            thread.run();

            while (true){

            }
        }

        public static void main(String[] args) throws InterruptedException {
            Main main = new Main();
        }
}