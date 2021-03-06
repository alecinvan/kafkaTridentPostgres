package storm.ingress.kafka.trident;

import storm.ingress.kafka.KafkaUtils;
import storm.trident.spout.IOpaquePartitionedTridentSpout;
import storm.trident.spout.IPartitionedTridentSpout;

import java.util.Map;

class Coordinator implements IPartitionedTridentSpout.Coordinator<GlobalPartitionInformation>, IOpaquePartitionedTridentSpout.Coordinator<GlobalPartitionInformation> {

    private IBrokerReader reader;
    private TridentKafkaConfig config;

    public Coordinator(Map conf, TridentKafkaConfig tridentKafkaConfig) {
        config = tridentKafkaConfig;
        reader = KafkaUtils.makeBrokerReader(conf, config);
    }

    @Override
    public void close() {
        config.coordinator.close();
    }

    @Override
    public boolean isReady(long txid) {
        return config.coordinator.isReady(txid);
    }

    @Override
    public GlobalPartitionInformation getPartitionsForBatch() {
        return reader.getCurrentBrokers();
    }
}
