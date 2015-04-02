
/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package algo.ad.runner;


import algo.ad.feeder.ArtificialTweetsEmitterSpout;
import algo.ad.processor.AnomalyDetectionBolt;
import algo.ad.processor.AnomalyDetectionBolt_PEWA_STDEV;
import algo.ad.processor.TweetAggregateBolt;
import algo.ad.utility.StormRunner;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;

/**
 * This topology does a continuous computation of the top N words that the topology has seen in terms of cardinality.
 * The top N computation is done in a completely scalable way, and a similar approach could be used to compute things
 * like trending topics or trending images on Twitter.
 */
public class AnomalyDetectionTopology {

  //private static final Logger LOG = Logger.getLogger(RollingTopWords.class);
  private static final int DEFAULT_RUNTIME_IN_SECONDS = 60;
  private static final int TOP_N = 5;

  private final TopologyBuilder builder;
  private final String topologyName;
  private final Config topologyConfig;
  private final int runtimeInSeconds;

  public AnomalyDetectionTopology(String topologyName) throws InterruptedException {
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
    String spoutId = "ArtificialTweetsEmitterSpout";
    String counterId = "TweetAggregateBolt";
    String intermediateRankerId = "AnomalyDetectionBolt";
  //  String totalRankerId = "finalRanker";
    builder.setSpout(spoutId, new ArtificialTweetsEmitterSpout(), 1);
    builder.setBolt(counterId, new TweetAggregateBolt(), 3).fieldsGrouping(spoutId, new Fields("sentiment_id"));
    builder.setBolt(intermediateRankerId, new AnomalyDetectionBolt_PEWA_STDEV(), 3).fieldsGrouping(counterId, new Fields(
        "sentiment_id"));
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
    AnomalyDetectionTopology rtw = new AnomalyDetectionTopology(topologyName);
    if (runLocally) {
     /// LOG.info("Running in local mode");
      rtw.runLocally();
    }
    else {
   //   LOG.info("Running in remote (cluster) mode");
      rtw.runRemotely();
    }
  }
}
