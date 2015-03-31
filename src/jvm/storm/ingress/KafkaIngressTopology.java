package storm.ingress;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.generated.StormTopology;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.IMetricsContext;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import clojure.lang.Numbers;
import com.google.common.collect.Lists;
import com.google.common.hash.HashFunction;
import com.google.common.hash.Hashing;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.ingress.kafka.BrokerHosts;
import storm.ingress.kafka.Broker;
import storm.ingress.kafka.StringScheme;
import storm.ingress.kafka.ZkHosts;
import storm.ingress.kafka.trident.OpaqueTridentKafkaSpout;
import storm.ingress.kafka.trident.TridentKafkaConfig;
import storm.ingress.kafka.trident.GlobalPartitionInformation;
import storm.ingress.kafka.StaticHosts;
import storm.ingress.postgresql.PostgresqlState;
import storm.ingress.postgresql.PostgresqlStateConfig;
import storm.ingress.postgresql.postgresConnector;
import storm.trident.TridentState;
import storm.trident.TridentTopology;
import storm.trident.fluent.GroupedStream;
import storm.trident.Stream;
import storm.trident.operation.BaseFilter;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.CombinerAggregator;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.spout.IBatchSpout;
import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.StateType;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.InputStream;
import java.util.Properties;
import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;
import java.sql.*;


public class KafkaIngressTopology {
         protected KafkaIngressTopology() // prevents calls from subclass
         {
                         throw new UnsupportedOperationException();
         }


         public static final Logger LOG = LoggerFactory.getLogger(KafkaIngressTopology.class);


           /**   Print message published by Kafka.  **/
	    @SuppressWarnings("serial")
		public static class PrintStream extends BaseFunction
        {

	   		@SuppressWarnings("rawtypes")

            @Override
            public void execute(TridentTuple tuple, TridentCollector collector)
            {
//                    String val = tuple.getString(0);
//                    System.out.println(tuple.getString(0) + " , " + tuple.getString(1));
                      System.out.println(tuple);
            }
        }

        @SuppressWarnings("serial")
        static class RandomTupleSpout implements IBatchSpout {
            private transient Random random;
            private static final int BATCH = 40;

            @Override
            @SuppressWarnings("rawtypes")
            public void open(final Map conf, final TopologyContext context) {
                          random = new Random();
            }

            @Override
            public void emitBatch(final long batchId, final TridentCollector collector) {
                  for (int i = 0; i < BATCH; i++) {
                          collector.emit(new Values("test string inserted into this table "+i));

                  }
            }

            @Override
            public void ack(final long batchId) {}

            @Override
            public void close() {}

            @Override
            @SuppressWarnings("rawtypes")
            public Map getComponentConfiguration() {
                   return null;
            }

            @Override
            public Fields getOutputFields() {
               //      return new Fields("userid", "event");
                     return new Fields("event");
            }
        }


                       /**  Parses JSON published by Kafka.   **/
        @SuppressWarnings("serial")
        public static class JsonObjectParse extends BaseFunction {

                   @Override
                   public final void execute(final TridentTuple tuple, final TridentCollector collector)
                   {
                       String val = tuple.getString(0);
                       System.out.println("++==++[[" + tuple + "]]");
                       System.out.println("==++==[[" + collector + "]]");

                       int userid;
                       try {
                           JSONObject json = new JSONObject(val);
                       
                           userid = json.getInt("userId") ;
//                         userid = json.getInt("user") ;
                       }
                       catch (JSONException e) {
                                 userid = -1 ;
                       }

                       collector.emit(new Values(userid, val));
                   }
        }


        @SuppressWarnings("serial")
        static class EventUpdater implements ReducerAggregator<List<String>> {

            @Override
            public List<String> init(){
                     return null;
            }

            @Override
            public List<String> reduce(List<String> curr, TridentTuple tuple) {
                   List<String> updated = null ;

                   if ( curr == null ) {
                                    String event = (String) tuple.getValue(1);
                                    System.out.println("===:" + event + ":");
                                    updated = Lists.newArrayList(event);
                   } else {
                                    System.out.println("===+" +  tuple + ":");
                                    updated = curr ;
                   }
//              System.out.println("(())");
              return updated ;
            }
        }


        public static List<String> CrunchifyGetPropertyValues() {
                        Properties prop = new Properties();
                          String propFileName = "./config/ingress.config";
                        //String propFileName = "/home/stuser/pof.analytics.messaging/kafka-storm-ingress/config/ingress.config";
                        try {
                                     InputStream inputStream = new FileInputStream(propFileName);
                                     prop.load(inputStream);
                        } catch (FileNotFoundException e) {
                            System.out.println("ERROR: FileNotFOund");
                                       e.printStackTrace();
                        } catch (IOException e) {
                            System.out.println("ERROR: IOError");
                                       e.printStackTrace();
                        }
                        String durl = prop.getProperty("db.url");
                        String brokerip = prop.getProperty("broker.ip");
                        String zkhosts = prop.getProperty("zk.host");
                        String topicname = prop.getProperty("topic.name");
                        List<String> configs = new ArrayList<String>();
                        configs.add(durl);
                        configs.add(brokerip);
                        configs.add(zkhosts);
                        configs.add(topicname);
                        return configs;
        }




        public static void main(String[] args) throws Exception {

                 String dburl = CrunchifyGetPropertyValues().get(0) ;
                 Broker broker = Broker.fromString(CrunchifyGetPropertyValues().get(1));
                 GlobalPartitionInformation info = new GlobalPartitionInformation();
//                 int partitionCount = 8;
//                 for(int i =0;i<partitionCount;i++){
//                                info.addPartition(i, broker) ;
//                 }

//                 StaticHosts hosts = new StaticHosts(info);
//                 StaticHosts kafkaHosts = KafkaConfig.StaticHosts.fromHostString(Arrays.asList(new String[] { "localhost" }), 1);
                 BrokerHosts zk = new ZkHosts(CrunchifyGetPropertyValues().get(2));
                 TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, CrunchifyGetPropertyValues().get(3));
//                 TridentKafkaConfig spoutConf = new TridentKafkaConfig(kafkaHosts, CrunchifyGetPropertyValues().get(3));
                 spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

//                 spoutConf.fetchSizeBytes = 50*1024*1024;
//                 spoutConf.bufferSizeBytes = 50*1024*1024;
                 spoutConf.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
//                spoutConf.startOffsetTime = kafka.api.OffsetRequest.LatestTime();
//                 spoutConf.socketTimeoutMs = 1000;
//                 spoutConf.fetchMaxWait = 1000;
                 spoutConf.forceFromStart = true;
//                 spoutConf.maxOffsetBehind = Long.MAX_VALUE;
//                 spoutConf.useStartOffsetTimeIfOffsetOutOfRange = true;
//                 spoutConf.metricsTimeBucketSizeInSecs = 600;

                 OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(spoutConf);
//                TransactionalTridentKafkaSpout kafkaSpout = new TransactionalTridentKafkaSpout(spoutConf);
                 TridentTopology topology = new TridentTopology();


                 final PostgresqlStateConfig config = new PostgresqlStateConfig();
                 {
                      config.setUrl(dburl);
                      config.setTable("input.events_storm_test");
                      config.setKeyColumns(new String[]{"id"});
                      config.setValueColumns(new String[]{"event_time", "event_type", "event_object"});
                      config.setType(StateType.NON_TRANSACTIONAL);
                      config.setCacheSize(5000);
                 }

                 topology.newStream("topictestspout", kafkaSpout)
//                            topology.newStream("test", new RandomTupleSpout())        // this test tells the kafkaSpout has the overhead to cause the latency
          //                            .parallelismHint(4)
//                                                .shuffle()
                                       .each(new Fields("str"),
                                             new JsonObjectParse(),
                                             new Fields("userid","event"))
//                                      .each(new Fields("event"),
//                                            new PrintStream(),
//                                            new Fields("word1"));
                                      .parallelismHint(6)
                                      .groupBy(new Fields("event"))
                                      .persistentAggregate(PostgresqlState.newFactory(config), new Fields("userid","event"), new EventUpdater(), new Fields( "eventword"));




                  Config conf = new Config();
//                  conf.setDebug(true);

                  if (args != null && args.length > 0) {
                          System.out.println("Storm cluster....");
                          conf.setNumWorkers(10);
//                          StormSubmitter.submitTopology(args[0], conf, buildTridentKafkaTopology());
                          StormSubmitter.submitTopology(args[0], conf, topology.build());
                  }
                  else {
                          System.out.println("local mode....");
                       //   conf.setMaxSpoutPending(10);
                          conf.setMaxTaskParallelism(10);
//                          LocalDRPC drpc = new LocalDRPC();
                          LocalCluster cluster = new LocalCluster();

                            System.out.println("++= V =++");
                          cluster.submitTopology("topictest", conf, topology.build());
                            System.out.println("++= ^ =++");
//                          while (true) {}

//                          Thread.sleep(1500);
//
//                          cluster.shutdown();
//                          drpc.shutdown();

                  }

	 	}


}
