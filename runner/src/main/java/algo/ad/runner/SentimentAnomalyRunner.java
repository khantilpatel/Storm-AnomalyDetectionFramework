package algo.ad.runner;

import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;
import java.util.Properties;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import nl.avisi.jms.SpringJmsProvider;

import org.apache.xbean.spring.context.ClassPathXmlApplicationContext;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.context.ApplicationContext;

import algo.ad.processor.AnomalyDetectionBolt_PEWA_STDEV;
import algo.ad.processor.SummarizeSentimentBolt;
import algo.ad.processor.TweetAggregateBolt;
import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.bolt.JmsBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Lists;

import common.feeder.spouts.ArtificialTweetsEmitterSpout;
import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.ConfigKeys;

/**
 * 
 * @author Khantil Patel
 *
 * ArtificialTweetsEmitterSpout >> TweetAggregateBolt >> AnomalyDetectionBolt_PEWA_STDEV >> SummarizeSentimentBolt
 */

public final class SentimentAnomalyRunner {

	public final static int SENTIMENT_LOG = 2;
	private static String propertiesFile = "jdbc.properties";
	public static Logger LOG = LoggerFactory.getLogger(SentimentAnomalyRunner.class);
	
	public static final void main(final String[] args) throws Exception {

		final ApplicationContext applicationContext = new ClassPathXmlApplicationContext(
				"applicationContext.xml");

		final JmsProvider jmsQueueProvider = new SpringJmsProvider(
				applicationContext, "jmsConnectionFactory", "notificationQueue");

		final TopologyBuilder topologyBuilder = new TopologyBuilder();
		
		
		final JmsBolt jmsBolt = new JmsBolt();
		jmsBolt.setJmsProvider(jmsQueueProvider);
		jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {

			@Override
			public final Message toMessage(final Session session,
					final Tuple tuple) throws JMSException {
				// final String json = "{\"word\":\"" + input.getString(0) +
				// "\", \"count\":" + String.valueOf(input.getInteger(1)) + "}";

				List<Object> otherFields = Lists.newArrayList(tuple.getValues());
				int current_positive_counter = (Integer) otherFields.get(0);
				int current_positive_anomaly = (Integer) otherFields.get(1);

				int current_negative_counter = (Integer) otherFields.get(2);
				int current_negative_anomaly = (Integer) otherFields.get(3);

				int current_neutral_counter = (Integer) otherFields.get(4);
				int current_neutral_anomaly = (Integer) otherFields.get(5);

				Date currentDate = (Date) otherFields.get(6); // 0: counter, 1:
																// sentement_Id,
																// 2:Date_object

				DateFormat dateFormat = new SimpleDateFormat(
						"yyyy/MM/dd HH:mm:ss");
				Date date = new Date();

				final String json = "{\"x\":\""
						+ dateFormat.format(currentDate) + "\", \"y0\":\""
						+ current_negative_counter + "\", \"a0\":\""
						+ current_negative_anomaly +

						"\", \"y2\":\"" + current_neutral_counter
						+ "\", \"a2\":\"" + current_neutral_anomaly +

						"\", \"y4\":\"" + current_positive_counter
						+ "\", \"a4\":\"" + current_positive_anomaly + "\"}";

				// System.out.println(json+"\n");
				return session.createTextMessage(json);
			}
		});

		String spoutId = "ArtificialTweetsEmitterSpout";
		String counterId = "TweetAggregateBolt";
		String intermediateRankerId = "AnomalyDetectionBolt";
		String summarizeSentimentId = "summarizeSentimentBolt";
		// String totalRankerId = "finalRanker";
		
		ApplicationConfigurationFile configFile = read(propertiesFile);
		
		topologyBuilder
				.setSpout(spoutId, new ArtificialTweetsEmitterSpout(true, configFile), 1);
		topologyBuilder.setBolt(counterId, new TweetAggregateBolt(), 3)
				.fieldsGrouping(spoutId, new Fields("sentiment_id"));
		topologyBuilder.setBolt(intermediateRankerId,
				new AnomalyDetectionBolt_PEWA_STDEV(), 3).fieldsGrouping(
				counterId, new Fields("sentiment_id"));
		topologyBuilder.setBolt(summarizeSentimentId,
				new SummarizeSentimentBolt()).shuffleGrouping(
				intermediateRankerId);
		topologyBuilder.setBolt("jmsBolt", jmsBolt).shuffleGrouping(
				summarizeSentimentId);

		// topologyBuilder.setSpout("wordGenerator", new RandomWordFeeder());
		// topologyBuilder.setBolt("counter", new
		// WordCounterBolt()).shuffleGrouping("wordGenerator");
		// topologyBuilder.setBolt("jmsBolt",
		// jmsBolt).shuffleGrouping("counter");

		final Config config = new Config();
		config.setDebug(false);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-count", config,
				topologyBuilder.createTopology());
	}
	
	private static ApplicationConfigurationFile read(String propertiesFile) {
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
