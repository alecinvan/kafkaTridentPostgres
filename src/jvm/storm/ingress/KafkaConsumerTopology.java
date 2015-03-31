package storm.ingress;

import backtype.storm.task.IMetricsContext;
import storm.ingress.kafka.BrokerHosts;
import storm.ingress.kafka.StringScheme;
import storm.ingress.kafka.ZkHosts;
import storm.ingress.kafka.trident.OpaqueTridentKafkaSpout;
import storm.ingress.kafka.trident.TridentKafkaConfig;
import storm.trident.TridentTopology;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.LocalDRPC;
import backtype.storm.StormSubmitter;
import backtype.storm.spout.SchemeAsMultiScheme;

import java.io.IOException;
import java.util.*;
import java.util.Map.Entry;

import backtype.storm.generated.StormTopology;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Values;

import org.json.JSONObject;
import org.json.JSONException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import storm.trident.operation.*;
import storm.trident.operation.builtin.Count;
import storm.trident.operation.builtin.MapGet;
import storm.trident.operation.builtin.Sum;
import storm.trident.state.ReadOnlyState;
import storm.trident.state.State;
import storm.trident.state.StateFactory;
import storm.trident.state.map.ReadOnlyMapState;
import storm.trident.tuple.TridentTuple;
import storm.trident.TridentState;

import com.google.common.hash.*;


public class KafkaConsumerTopology {
         protected KafkaConsumerTopology() // prevents calls from subclass
         {
                         throw new UnsupportedOperationException();
         }


         public static final Logger LOG = LoggerFactory.getLogger(KafkaConsumerTopology.class);


              /**  split sentence to words   **/
         @SuppressWarnings("serial")
         public static class Split extends BaseFunction {

                    @Override
                    public void execute(TridentTuple tuple, TridentCollector collector) {
                             String sentence = tuple.getString(0);
                             for(String word: sentence.split(" ")) {
                                        collector.emit(new Values(word));
                             }
                    }
         }

	       /**  Parses JSON published by Kafka.   **/
	     @SuppressWarnings("serial")
	     public static class JsonObjectParse extends BaseFunction {

	           @Override
	           public final void execute(final TridentTuple tuple, final TridentCollector collector)
               {
                   String val = tuple.getString(0);
                   String userid;
                   JSONObject json = new JSONObject(val);
                   try {
                         userid = Integer.toString(json.getInt("userid") )  ;
                   }
                   catch (JSONException e) {
                //         System.err.println("Caught JSONException: " + e.getMessage());
                         userid = "anonymous";
                   }

//                   System.out.println("\"time\":" + json.getInt("time") +
//                                      ",\"userid\":\"" + userid + "\"" +
//                                      ",\"pagename\":" + json.getString("pagename") + "\n"
//                                      );
                   collector.emit(new Values(json.getInt("time"), userid, json.getString("pagename")));
	           }
        }

           /**   Print message published by Kafka.  **/
	    @SuppressWarnings("serial")
		public static class PrintStream extends BaseFunction
        {

	   		@SuppressWarnings("rawtypes")

            @Override
            public void execute(TridentTuple tuple, TridentCollector collector)
            {
//                    String val = tuple.getString(0);
                    System.out.println(tuple);
            }
	    }


        public static class StaticSingleKeyMapState extends ReadOnlyState implements ReadOnlyMapState<Object> {
             public static class Factory implements StateFactory {
                  Map _map;

                  public Factory(Map map) {
                        _map = map;
                  }

                  @Override
                  public State makeState(Map conf, IMetricsContext metrics, int partitionIndex, int numPartitions) {
                            return new StaticSingleKeyMapState(_map);
                  }

             }

             Map _map;

             public StaticSingleKeyMapState(Map map) {
                  _map = map;
             }


             @Override
             public List<Object> multiGet(List<List<Object>> keys) {
                 List<Object> ret = new ArrayList();
                 for (List<Object> key : keys) {
                       Object singleKey = key.get(0);
                       ret.add(_map.get(singleKey));
                 }
                return ret;
             }

        }

            /**  Storm topology   **/
        public static StormTopology buildTridentKafkaTopology() throws IOException {
	       		BrokerHosts zk = new ZkHosts("10.100.70.128:2181");
	       		TridentKafkaConfig spoutConf = new TridentKafkaConfig(zk, "topictest");
	       		spoutConf.scheme = new SchemeAsMultiScheme(new StringScheme());

                     /***  two types of tridentKafkaSpout ******/
	       		OpaqueTridentKafkaSpout spout = new OpaqueTridentKafkaSpout(spoutConf);
//              TransactionalTridentKafkaSpout spout = new TransactionalTridentKafkaSpout(spoutConf);
                     /*****************/

                TridentTopology topology = new TridentTopology();
//                TridentState totalCountState =
                topology.newStream("topictestspout", spout).shuffle()
	  //                                         .each(new Fields("str"),
       //                                              new JsonObjectParse(),
	   //                                              new Fields("time", "userid", "pagename"))
      //                                         .groupBy(new Fields("userid"))
      //                                         .aggregate(new Fields("userid"), new Count(), new Fields("count"))
                                               .each(new Fields("str"),
                                                     new PrintStream(),
                                                     new Fields("events"))
 //                                              .persistentAggregate(new MemoryMapState.Factory(),
 //                                                                   new Count(),
 //                                                                   new Fields("count"))
                                               .parallelismHint(16);

	            return topology.build();

        }


        public static class testHLL extends BaseFunction
        {

            @SuppressWarnings("rawtypes")

            final int seed = 123456;
            final HashFunction hash = Hashing.murmur3_128(seed);
            final int REGWIDTH = 5 ;
            final int LOG2M = 13 ;

            final int REGISTER_COUNT = (1 << LOG2M);
            final int REGISTER_MAX_VALUE = (1 << REGWIDTH) - 1;

//            final Hasher hasher = hash.newHasher();
//            final HLL hll = new HLL(LOG2M, REGWIDTH);


            @Override
            public void execute(TridentTuple tuple, TridentCollector collector)
            {
                    String val = tuple.getString(0);
                    System.out.println(val);
                    collector.emit(new Values(val));
            }

        }


        public static Map<String, List<String>> PAGEVIEW_DB = new HashMap<String, List<String>>() {{
                  put("upgrade.aspx", Arrays.asList("Sa", "Fitsum", "Martin", "Nisan", "Steve", "Sa", "Fitsum"));
                  put("meetme.aspx", Arrays.asList("Tommy", "Martin", "Sa", "Steve", "Martin"));
                  put("inbox.aspx", Arrays.asList("Chris", "Shawn", "Steve"));
        }};


        public static class ExpandList extends BaseFunction {

                @Override
                public void execute(TridentTuple tuple, TridentCollector collector) {
                      List l = (List) tuple.getValue(0);
                      if (l != null) {
                           for (Object o : l) {
                             collector.emit(new Values(o));
                           }
                      }
                }

        }

        public static class One implements CombinerAggregator<Integer> {
                @Override
                public Integer init(TridentTuple tuple) {
                        return 1;
                }

                @Override
                public Integer combine(Integer val1, Integer val2) {
                        return 1;
                }

                @Override
                public Integer zero() {
                        return 1;
                }
        }

        public static StormTopology buildTridentReachTopology(LocalDRPC drpc) {
                TridentTopology topology = new TridentTopology();
                TridentState urlToVisitors = topology.newStaticState(new StaticSingleKeyMapState.Factory(PAGEVIEW_DB));

                topology.newDRPCStream("reach", drpc).stateQuery(urlToVisitors, new Fields("args"), new MapGet(), new Fields(
                        "visitors")).each(new Fields("visitors"),
                                          new ExpandList(),
                                          new Fields("visitor")).shuffle()
                                    .groupBy(new Fields("visitor"))
                                    .aggregate(new One(), new Fields("one"))
                                    .aggregate(new Fields("one"), new Sum(), new Fields("reach"));

                return topology.build();
        }


        public static void main(String[] args) throws Exception {

         	 	  Config conf = new Config();
//	 	          conf.setDebug(true);
	         	  conf.setMaxSpoutPending(10);
	        	  conf.setMaxTaskParallelism(3);
	        	  LocalDRPC drpc = new LocalDRPC();
	         	  LocalCluster cluster = new LocalCluster();

                  System.out.print(PAGEVIEW_DB.toString()+"\n");

                  if (args != null && args.length > 0) {
                          cluster.submitTopology("topictest", conf, buildTridentReachTopology(drpc));

                          Thread.sleep(1500);
//                          cluster.shutdown();
//                          drpc.shutdown();
                  }
                  else {
                          conf.setNumWorkers(3);
                          StormSubmitter.submitTopology(args[0], conf, buildTridentKafkaTopology());
                  }

	 	}


        public final static Map<String, Integer> getTopNOfMap(Map<String, Integer> map, int n)
        {
                  List<Map.Entry<String, Integer>> entryList = new ArrayList<Map.Entry<String, Integer>>(map.size());
                  entryList.addAll(map.entrySet());
                  Collections.sort(entryList, new Comparator<Map.Entry<String, Integer>>() {

                              @Override
                              public int compare(Entry<String, Integer> arg0, Entry<String, Integer> arg1) {
                                      return arg1.getValue().compareTo(arg0.getValue());
                              }
                  });

                  Map<String, Integer> toReturn = new HashMap<String, Integer>();
                  for(Map.Entry<String, Integer> entry: entryList.subList(0, Math.min(entryList.size(), n))) {
                                 toReturn.put(entry.getKey(), entry.getValue());
                  }
                  return toReturn;
        }


}
