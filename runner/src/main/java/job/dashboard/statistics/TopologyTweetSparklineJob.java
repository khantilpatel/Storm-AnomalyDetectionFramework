package job.dashboard.statistics;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.Properties;

import job.dashboard.statistics.processor.BoltAggregateSparklineData;
import job.dashboard.statistics.processor.BoltSaveSparklineJSONToMySQL;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import algo.ad.utility.StormRunner;
import backtype.storm.Config;
import backtype.storm.topology.TopologyBuilder;
import common.feeder.spouts.SpoutTweetSparkline;
import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.ConfigKeys;

public class TopologyTweetSparklineJob {

	// private static final Logger LOG =
	// Logger.getLogger(RollingTopWords.class);
	private static final int DEFAULT_RUNTIME_IN_SECONDS = 2000;
	// private static final int TOP_N = 5;

	private final TopologyBuilder builder;
	private final String topologyName;
	private final Config topologyConfig;
	private final int runtimeInSeconds;

	private static String propertiesFile = "jdbc.properties";
	
	public static Logger LOG = LoggerFactory
			.getLogger(TopologyTweetSparklineJob.class);

	public TopologyTweetSparklineJob(String topologyName)
			throws InterruptedException, FileNotFoundException {
		builder = new TopologyBuilder();
		this.topologyName = topologyName;
		topologyConfig = createTopologyConfiguration();
		runtimeInSeconds = DEFAULT_RUNTIME_IN_SECONDS;

		wireTopology();
	}

	private static Config createTopologyConfiguration() {
		Config conf = new Config();
		conf.setDebug(false);
		return conf;
	}

	private void wireTopology() throws InterruptedException, FileNotFoundException {
		String spoutId_TweetSparklineSpout = "TweetSparklineSpout";

		String boltId_AggregateSparklineData = "BoltAggregateSparklineData";

		String boltId_BoltSaveSparklineJSONToMySQL = "BoltSaveSparklineJSONToMySQL";
		// TopologyBuilder builder = new TopologyBuilder();
		boolean isDebug = true;// no database inserts
		
		ApplicationConfigurationFile configFile = read(propertiesFile);		
		
		builder.setSpout(spoutId_TweetSparklineSpout, new SpoutTweetSparkline(
				configFile, isDebug), 1);
		builder.setBolt(
				boltId_AggregateSparklineData,
				new BoltAggregateSparklineData(configFile, isDebug), 1).shuffleGrouping(
				spoutId_TweetSparklineSpout);
		builder.setBolt(
				boltId_BoltSaveSparklineJSONToMySQL,
				new BoltSaveSparklineJSONToMySQL(configFile, isDebug), 1).shuffleGrouping(
				boltId_AggregateSparklineData);
	}

	public void runLocally() throws InterruptedException {
		StormRunner.runTopologyLocally(builder.createTopology(), topologyName,
				topologyConfig, runtimeInSeconds);
	}

	public void runRemotely() throws Exception {
		StormRunner.runTopologyRemotely(builder.createTopology(), topologyName,
				topologyConfig);
	}

	/**
	 * Submits (runs) the topology.
	 *
	 * Usage: "RollingTopWords [topology-name] [local|remote]"
	 *
	 * By default, the topology is run locally under the name
	 * "slidingWindowCounts".
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
	 * @param args
	 *            First positional argument (optional) is topology name, second
	 *            positional argument (optional) defines whether to run the
	 *            topology locally ("local") or remotely, i.e. on a real cluster
	 *            ("remote").
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

		// LOG.info("Topology name: " + topologyName);
		TopologyTweetSparklineJob rtw = new TopologyTweetSparklineJob(
				topologyName);
		if (runLocally) {
			// / LOG.info("Running in local mode");
			rtw.runLocally();
		} else {
			// LOG.info("Running in remote (cluster) mode");
			rtw.runRemotely();
		}
	}
	
	private ApplicationConfigurationFile read(String propertiesFile) {
		ApplicationConfigurationFile configFile = new ApplicationConfigurationFile();
		try {
			
			FileInputStream inputStream = new FileInputStream(propertiesFile);
			
			Properties properties = new Properties();

			properties.load(inputStream);

			String jdbc_username_override = properties
					.getProperty(ConfigKeys.JDBC_USERNAME);
			if (jdbc_username_override != null) {
				configFile.setJdbc_username(jdbc_username_override);
			}
			LOG.info("Using jdbc_username:: " + jdbc_username_override);

			String jdbc_password_override = properties
					.getProperty(ConfigKeys.JDBC_PASSWORD);
			if (jdbc_password_override != null) {
				configFile.setJdbc_password(jdbc_password_override);
			}
			LOG.info("Using jdbc_password:: " + jdbc_password_override);

			String jdbc_url_override = properties
					.getProperty(ConfigKeys.JDBC_URL);
			if (jdbc_url_override != null) {
				configFile.setJdbc_url(jdbc_url_override);
			}
			LOG.info("Using jdbc_url:: " + jdbc_url_override);

			/*
			 * Related to MongoDB
			 */

			String mongodb_base_url_override = properties
					.getProperty(ConfigKeys.MONGODB_URL);
			if (mongodb_base_url_override != null) {
				configFile.setMongodb_base_url(mongodb_base_url_override);
			}
			LOG.info("Using mongodb_base_url:: " + mongodb_base_url_override);

			String mongodb_username_override = properties
					.getProperty(ConfigKeys.MONGODB_USERNAME);
			if (mongodb_username_override != null) {
				configFile.setMongodb_username(mongodb_username_override);
			}
			LOG.info("Using mongodb_username:: " + mongodb_username_override);

			String mongodb_password_override = properties
					.getProperty(ConfigKeys.MONGODB_PASSWORD);
			if (mongodb_password_override != null) {
				configFile.setMongodb_password(mongodb_password_override);
			}
			LOG.info("Using mongodb_password:: " + mongodb_password_override);

			String mongodb_port_override = properties
					.getProperty(ConfigKeys.MONGODB_PORT);
			if (mongodb_port_override != null) {
				configFile.setMongodb_port(mongodb_port_override);
			}
			LOG.info("Using mongodb_password:: " + mongodb_port_override);

			// Sentiment140 Stuff

			String sentiment140_url_override = properties
					.getProperty(ConfigKeys.SENTIMENT140_URL);
			if (sentiment140_url_override != null) {
				configFile.setSentiment140_url(sentiment140_url_override);
			}
			LOG.info("Using sentiment140_url_override:: "
					+ sentiment140_url_override);

			// Twitter API Key stuff

			String twitter_ck_override = properties
					.getProperty(ConfigKeys.TWITTER_CONSUMER_KEY);
			if (twitter_ck_override != null) {
				configFile.setTwitter_consumerKey(twitter_ck_override);
			}
			LOG.info("Using twitter_ck_override:: " + twitter_ck_override);

			String twitter_cs_override = properties
					.getProperty(ConfigKeys.TWITTER_CONSUMER_SECRET);
			if (twitter_cs_override != null) {
				configFile.setTwitter_consumerSecret(twitter_cs_override);
			}
			LOG.info("Using twitter_cs_override:: " + twitter_cs_override);

			String twitter_at_override = properties
					.getProperty(ConfigKeys.TWITTER_ACCESS_TOKEN);
			if (twitter_at_override != null) {
				configFile.setTwitter_accessToken(twitter_at_override);
			}
			LOG.info("Using twitter_at_override:: " + twitter_at_override);

			String twitter_ats_override = properties
					.getProperty(ConfigKeys.TWITTER_ACCESS_TOKEN_SECRET);
			if (twitter_ats_override != null) {
				configFile.setTwitter_accessTokenSecret(twitter_ats_override);
			}
			LOG.info("Using twitter_ats_override:: " + twitter_ats_override);

			// PARAMETERS for SPARKLINE DURATION

			String sparkline_window_period_override = properties
					.getProperty(ConfigKeys.SPARKILNE_WINDOW_PERIOD);
			if (sparkline_window_period_override != null) {
				configFile.setSparkline_window_period(Integer
						.valueOf(sparkline_window_period_override));
			}
			LOG.info("Using sparkline_window_period:: "
					+ sparkline_window_period_override);

			String sparkline_aggregation_period_override = properties
					.getProperty(ConfigKeys.SPARKILNE_AGGREGATION_PERIOD);
			if (sparkline_aggregation_period_override != null) {
				configFile.setSparkline_aggregation_period(Integer
						.valueOf(sparkline_aggregation_period_override));
			}
			LOG.info("Using sparkline_aggregation_period:: "
					+ sparkline_aggregation_period_override);
			
			String sparkline_refresh_interval_override = properties
					.getProperty(ConfigKeys.SPARKILNE_REFRESH_PERIOD);
			if (sparkline_refresh_interval_override != null) {
				configFile.setSparkline_refresh_interval(Integer
						.valueOf(sparkline_refresh_interval_override));
			}
			LOG.info("Using sparkline_refresh_interval_override:: "
					+ sparkline_refresh_interval_override);

			
		} catch (FileNotFoundException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return configFile;
	}

}
