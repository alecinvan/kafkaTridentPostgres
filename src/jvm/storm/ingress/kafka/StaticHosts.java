package storm.ingress.kafka;

import storm.ingress.kafka.trident.GlobalPartitionInformation;


public class StaticHosts implements BrokerHosts {


    private GlobalPartitionInformation partitionInformation;

    public StaticHosts(GlobalPartitionInformation partitionInformation) {
        this.partitionInformation = partitionInformation;
    }

    public GlobalPartitionInformation getPartitionInformation() {
        return partitionInformation;
    }
}
