package storm.ingress;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;
import backtype.storm.task.TopologyContext;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;
import com.google.common.collect.Lists;
import org.json.JSONException;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import storm.ingress.kafka.Broker;
import storm.ingress.kafka.BrokerHosts;
import storm.ingress.kafka.StringScheme;
import storm.ingress.kafka.ZkHosts;
import storm.ingress.kafka.trident.GlobalPartitionInformation;
import storm.ingress.kafka.trident.OpaqueTridentKafkaSpout;
import storm.ingress.kafka.trident.TridentKafkaConfig;
import storm.ingress.postgresql.PostgresqlStateConfig;
import storm.trident.operation.builtin.Count;
import storm.ingress.postgresql.PostgresqlState;
import storm.trident.TridentTopology;
import storm.trident.operation.BaseFunction;
import storm.trident.operation.ReducerAggregator;
import storm.trident.operation.TridentCollector;
import storm.trident.testing.MemoryMapState;
import storm.trident.spout.IBatchSpout;
import storm.trident.state.StateType;
import storm.trident.tuple.TridentTuple;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.*;
import java.util.regex.Pattern;


public class KafkaIngresBasic {
    protected KafkaIngresBasic() // prevents calls from subclass
    {
        throw new UnsupportedOperationException();
    }

    public static final Logger LOG = LoggerFactory.getLogger(KafkaIngresBasic.class);


    /**
     * Parses JSON published by Kafka.   *
     */
    @SuppressWarnings("serial")
    public static class JsonObjectParse extends BaseFunction {
        static int counter = 0;

        @Override
        public final void execute(final TridentTuple tuple, final TridentCollector collector) {
            String event = tuple.getString(0);
            String eventType = null;
            counter++;

            try {
                JSONObject json = new JSONObject(event);

                eventType = json.getString("eventType");
                System.out.println("<<<" + counter);
            } catch (JSONException e) {
                // we have an error, need to do something to raise this up the chain
                System.out.println("<<<" + counter + " ERROR: " + e.getMessage());
            }
            collector.emit(new Values(eventType, event));
        }
    }

    @SuppressWarnings("serial")
    static class EventUpdater implements ReducerAggregator<List<String>> {
        static int counter = 0;
        static int duplicates = 0;

        @Override
        public List<String> init() {
            return null;
        }

        @Override
        public List<String> reduce(List<String> curr, TridentTuple tuple) {
            List<String> updated = null;
            counter++;

            if (curr == null) {
                String event = (String) tuple.getValue(0);
                updated = Lists.newArrayList(event);
                System.out.println(">>>" + counter);

            } else {
                updated = curr;
                duplicates++;
                System.out.println(">>> [[" + duplicates + "]]" + counter);
            }

            return updated;
        }
    }


    public static List<String> CrunchifyGetPropertyValues() {
        Properties prop = new Properties();
        String propFileName = "./config/ingress.config";
        try {
            InputStream inputStream = new FileInputStream(propFileName);
            prop.load(inputStream);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        } catch (IOException e) {
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

        Map<String, String> parameters = new HashMap<String, String>();
        int runMode = 0; // run Mode 0 is local mode, 1 is storm cluster
        String lastArg = null;
        System.out.println("{args: " + args.length + "}");

        if (args != null && args.length > 0) {
            System.out.println("Checking Parameters...");
            // nasty but simple poor man command line parser....
            for (int i = args.length - 1; i >= 0; i--) {
                System.out.println("[" + i + "]" + args[i]);
                if (args[i].startsWith("--")) {
                    parameters.put(args[i].substring(2).toLowerCase(), lastArg);
                    System.out.println("{" + args[i] + "/" + args[i].substring(2).toLowerCase() + "} => " + lastArg);

                } else if (args[i].startsWith("-")) {
                    parameters.put(args[i].substring(1).toLowerCase(), lastArg);
                    System.out.println("[" + args[i] + "/" + args[i].substring(1).toLowerCase() + "] => " + lastArg);
                }

                if (args[i].charAt(0) == '-') {
                    lastArg = null;
                } else {
                    lastArg = args[i];
                }
            }

            // capture the topology name
            if (lastArg != null && !parameters.containsKey("topology")) {
                parameters.put("topology", lastArg);
            }
        }

        if (parameters.containsKey("help") || parameters.containsKey("h")) {
            // help requested....
            System.out.println("...\\t[--table <tableName>]\\n\\t[--topic <topicName>]\\n\\t[--brokerip <brokerip>]\\n\\t[--zkhosts <zkhost[,zkhost[,...]]>]\\n\\t[--forcestart True|False]\\n\\t[parallelism <n>]\\n\\t[[--topology] <topologyName>]");
            return;
        }

        // add the cruncify parameters to the parameters dictionary
        if (!parameters.containsKey("dburl")) {
            parameters.put("dburl", CrunchifyGetPropertyValues().get(0));
        }
        if (!parameters.containsKey("brokerip")) {
            parameters.put("brokerip", CrunchifyGetPropertyValues().get(1));
        }
        if (!parameters.containsKey("zkhosts")) {
            parameters.put("zkhosts", CrunchifyGetPropertyValues().get(2));
        }
        if (!parameters.containsKey("topic")) {
            parameters.put("topic", CrunchifyGetPropertyValues().get(3));
        }
        if (!parameters.containsKey("forcestart")) {
            parameters.put("forcestart", "False");
        }
        if (!parameters.containsKey("parallelism")) {
            parameters.put("parallelism", "8");
        }

        // set up the topic
        Broker broker = Broker.fromString(parameters.get("brokerip"));
        GlobalPartitionInformation info = new GlobalPartitionInformation();
        BrokerHosts zk = new ZkHosts(parameters.get("zkhosts")); //CrunchifyGetPropertyValues().get(2));
        System.out.println("topic=>" + parameters.get("topic")); //CrunchifyGetPropertyValues().get(3));
        TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, parameters.get("topic"));
        spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

        spoutConf.startOffsetTime = kafka.api.OffsetRequest.EarliestTime();
        if (parameters.get("forcestart").equalsIgnoreCase("true")) {
            spoutConf.forceFromStart = true;
        }

        OpaqueTridentKafkaSpout kafkaSpout = new OpaqueTridentKafkaSpout(spoutConf);
        TridentTopology topology = new TridentTopology();

        final PostgresqlStateConfig config = new PostgresqlStateConfig();
        {
            config.setUrl(parameters.get("dburl"));

            if (!parameters.containsKey("table")) {
                config.setTable("input.events_storm_test");
            } else {
                config.setTable(parameters.get("table"));
            }

            config.setKeyColumns(new String[]{"event_object"});
            config.setValueColumns(new String[]{"event_type"});

            config.setType(StateType.NON_TRANSACTIONAL);
            config.setCacheSize(5000);
        }

        // parse out the parallelism hint value
        int pHint;
        try {
            pHint = Integer.parseInt(parameters.get("parallelism"));
        } catch (NumberFormatException e) {
            pHint = 8;
        }

        topology.newStream("spoutInit", kafkaSpout)
                //.each(new Fields("str"), new PrintStream())
                //.groupBy(new Fields("str"))
                //.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("srcCount"))
                .each(new Fields("str"),
                        new JsonObjectParse(),
                        new Fields("eventType", "event"))
                .parallelismHint(pHint)
                        //.groupBy(new Fields("appSessionId", "eventId"))
                        //.groupBy(new Fields("eventTime","eventType"))
                .groupBy(new Fields("event"))
                        //.persistentAggregate(new MemoryMapState.Factory(), new Count(), new Fields("destCount"))
                .persistentAggregate(PostgresqlState.newFactory(config), new Fields("eventType"), new EventUpdater(), new Fields("eventWord"))
        ;


        Config conf = new Config();

        if (parameters.containsKey("topology")) {
            System.out.println("Storm cluster....");
            conf.setNumWorkers(10);
            StormSubmitter.submitTopology(parameters.get("topology"), conf, topology.build());

        } else {
            System.out.println("local mode....");
            conf.setMaxTaskParallelism(10);
            LocalCluster cluster = new LocalCluster();
            cluster.submitTopology("apiTop", conf, topology.build());

        }
    }
}

