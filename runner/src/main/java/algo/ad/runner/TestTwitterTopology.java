package algo.ad.runner;

import algo.ad.feeder.TwitterStreamSpout;
import algo.ad.processor.PrinterBolt;
import algo.ad.processor.Sentiment140Bolt;
import algo.ad.utility.StormRunner;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;


public class TestTwitterTopology {
		 

	  //private static final Logger LOG = Logger.getLogger(RollingTopWords.class);
	  private static final int DEFAULT_RUNTIME_IN_SECONDS = 2000;
	  private static final int TOP_N = 5;

	  private final TopologyBuilder builder;
	  private final String topologyName;
	  private final Config topologyConfig;
	  private final int runtimeInSeconds;

	  public TestTwitterTopology(String topologyName) throws InterruptedException {
	    builder = new TopologyBuilder();
	    this.topologyName = topologyName;
	    topologyConfig = createTopologyConfiguration();
	    runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

	    wireTopology();
	  }

	  private static Config createTopologyConfiguration() {
	    Config conf = new Config();
	    conf.setDebug(true);
	    return conf;
	  }

	  private void wireTopology() throws InterruptedException {
	    String spoutId = "twitterStream";
	    String printId = "print";
	   // String intermediateRankerId = "AnomalyDetectionBolt";
	    
	    String consumerKey = "8Yp9j3R1oD6WeAeqKLMuA"; //args[0]; 
        String consumerSecret = "9SbeR8422jgT3njhZ7hP2og8ttIICD2VThraXn22Vw";//args[1]; 
        String accessToken = "61982388-L4t44vkyCGgANYREol9T7L6TwKiVd9mQkw3xBdZ3c";//args[2]; 
        String accessTokenSecret = "2bqJKti7KatTOSGQL7Z5knuqyCVtdN0MOGkeVsjkn5G7F";// args[3];
        String[] keyWords = {"#BieberRoast"};// Arrays.copyOfRange(arguments, 4, arguments.length);
        
      //  TopologyBuilder builder = new TopologyBuilder();
        
        builder.setSpout(spoutId, new TwitterStreamSpout(consumerKey, consumerSecret,
                                accessToken, accessTokenSecret, keyWords),1);
        builder.setBolt(printId, new Sentiment140Bolt())
        .shuffleGrouping(spoutId);
        
	  //  String totalRankerId = "finalRanker";
	//    builder.setSpout(spoutId, new ArtificialTweetsEmitterSpout(), 1);
	   // builder.setBolt(counterId, new TweetAggregateBolt(), 3).fieldsGrouping(spoutId, new Fields("sentiment_id"));
	 //   builder.setBolt(intermediateRankerId, new AnomalyDetectionBolt_PEWA_STDEV(), 3).fieldsGrouping(counterId, new Fields(
	 //       "sentiment_id"));
	  //  builder.setBolt(totalRankerId, new TotalRankingsBolt(TOP_N)).globalGrouping(intermediateRankerId);
	  }

	  public void runLocally() throws InterruptedException {
	    StormRunner.runTopologyLocally(builder.createTopology(), topologyName, topologyConfig, runtimeInSeconds);
	  }

	  public void runRemotely() throws Exception {
	    StormRunner.runTopologyRemotely(builder.createTopology(), topologyName, topologyConfig);
	  }

	  /**
	   * Submits (runs) the topology.
	   *
	   * Usage: "RollingTopWords [topology-name] [local|remote]"
	   *
	   * By default, the topology is run locally under the name "slidingWindowCounts".
	   *
	   * Examples:
	   *
	   * <pre>
	   * {@code
	   *
	   * # Runs in local mode (LocalCluster), with topology name "slidingWindowCounts"
	   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords
	   *
	   * # Runs in local mode (LocalCluster), with topology name "foobar"
	   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords foobar
	   *
	   * # Runs in local mode (LocalCluster), with topology name "foobar"
	   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords foobar local
	   *
	   * # Runs in remote/cluster mode, with topology name "production-topology"
	   * $ storm jar storm-starter-jar-with-dependencies.jar storm.starter.RollingTopWords production-topology remote
	   * }
	   * </pre>
	   *
	   * @param args First positional argument (optional) is topology name, second positional argument (optional) defines
	   *             whether to run the topology locally ("local") or remotely, i.e. on a real cluster ("remote").
	   * @throws Exception
	   */
	  public static void main(String[] args) throws Exception {
	    String topologyName = "AnomalyDetection";
	    if (args.length >= 1) {
	      topologyName = args[0];
	    }
	    boolean runLocally = true;
	    if (args.length >= 2 && args[1].equalsIgnoreCase("remote")) {
	      runLocally = false;
	    }

	    //LOG.info("Topology name: " + topologyName);
	    TestTwitterTopology rtw = new TestTwitterTopology(topologyName);
	    if (runLocally) {
	     /// LOG.info("Running in local mode");
	      rtw.runLocally();
	    }
	    else {
	   //   LOG.info("Running in remote (cluster) mode");
	      rtw.runRemotely();
	    }
	  }
	  
//	  public static void main(String[] args) {
//		 	String consumerKey = "8Yp9j3R1oD6WeAeqKLMuA"; //args[0]; 
//	        String consumerSecret = "9SbeR8422jgT3njhZ7hP2og8ttIICD2VThraXn22Vw";//args[1]; 
//	        String accessToken = "61982388-L4t44vkyCGgANYREol9T7L6TwKiVd9mQkw3xBdZ3c";//args[2]; 
//	        String accessTokenSecret = "2bqJKti7KatTOSGQL7Z5knuqyCVtdN0MOGkeVsjkn5G7F";// args[3];
//	        String[] arguments = args.clone();
//	        String[] keyWords = {"@azsuperbowl","#SB49"};// Arrays.copyOfRange(arguments, 4, arguments.length);
//	        
//	        TopologyBuilder builder = new TopologyBuilder();
//	        
//	        builder.setSpout("spoutId", new TwitterSampleSpout(consumerKey, consumerSecret,
//	                                accessToken, accessTokenSecret, keyWords));
//	        builder.setBolt("print", new PrinterBolt())
//	                .shuffleGrouping("spout");
//	                
//	                
//	        Config conf = new Config();
//	        
//	        
//	        LocalCluster cluster = new LocalCluster();
//	        
//	        cluster.submitTopology("test", conf, builder.createTopology());
//	        
//	        Utils.sleep(10000);
//	        cluster.shutdown();
//	    }
}


