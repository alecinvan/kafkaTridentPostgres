package storm.ingress.kafka.trident;

public interface IBrokerReader {

    GlobalPartitionInformation getCurrentBrokers();

    void close();
}
