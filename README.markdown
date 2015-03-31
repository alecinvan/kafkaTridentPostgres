# Author: Sa Li 
# kafka-postgresql trident writer

This is a kafka-writer to consume data from kafka producer and write data into postgresql DB (ingress).
The topology takes storm TridentKafkaSpout adn persistentAggregate. 

Speed test needs to be done. 


Command to run this writer on server:

storm jar target/kafka-storm-ingress-0.0.1-SNAPSHOT-jar-with-dependencies.jar storm.ingress.KafkaIngressTopology KafkaIngress


