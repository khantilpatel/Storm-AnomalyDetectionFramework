package algo.ad.runner;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.List;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import nl.avisi.jms.SpringJmsProvider;

import org.apache.xbean.spring.context.ClassPathXmlApplicationContext;
import org.springframework.context.ApplicationContext;

import com.google.common.collect.Lists;

import algo.ad.feeder.ArtificialTweetsEmitterSpout;
import algo.ad.processor.AnomalyDetectionBolt;
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

/**
 * @author robbreuk
 * @author Prashanth
 */

public final class SentimentAnomalyRunner {
	
	public final static int SENTIMENT_LOG = 2;
	
	public static final void main(final String[] args) throws Exception {
		final ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");

		final JmsProvider jmsQueueProvider = new SpringJmsProvider(applicationContext, "jmsConnectionFactory",
				                                                          "notificationQueue");

		final TopologyBuilder topologyBuilder = new TopologyBuilder();

		final JmsBolt jmsBolt = new JmsBolt();
		jmsBolt.setJmsProvider(jmsQueueProvider);
		jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {
			
			@Override
			public final Message toMessage(final Session session, final Tuple tuple) throws JMSException {
				//final String json = "{\"word\":\"" + input.getString(0) + "\", \"count\":" + String.valueOf(input.getInteger(1)) + "}";
				
				  List<Object> otherFields = Lists.newArrayList(tuple.getValues());
				  int current_positive_counter = (Integer) otherFields.get(0);
				  int current_positive_anomaly = (Integer) otherFields.get(1);
				  
				  int current_negative_counter = (Integer) otherFields.get(2);
				  int current_negative_anomaly = (Integer) otherFields.get(3);
				  
				  int current_neutral_counter = (Integer) otherFields.get(4);
				  int current_neutral_anomaly = (Integer) otherFields.get(5);
				  
				  Date currentDate = (Date) otherFields.get(6); // 0: counter, 1: sentement_Id, 2:Date_object
				  
				DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Date date = new Date();
			
				final String json = 
						"{\"x\":\"" + dateFormat.format(currentDate) + 
						"\", \"y0\":\"" +  current_negative_counter+ 
						"\", \"a0\":\"" +  current_negative_anomaly+
						
						"\", \"y2\":\"" +  current_neutral_counter +
						"\", \"a2\":\"" +  current_neutral_anomaly +
						
						"\", \"y4\":\"" +  current_positive_counter +
						"\", \"a4\":\"" +  current_positive_anomaly +
						"\"}";
				
				//System.out.println(json+"\n");
				return session.createTextMessage(json);
			}
		});

	    String spoutId = "ArtificialTweetsEmitterSpout";
	    String counterId = "TweetAggregateBolt";
	    String intermediateRankerId = "AnomalyDetectionBolt";
	    String summarizeSentimentId = "summarizeSentimentBolt";
	  //  String totalRankerId = "finalRanker";
	    topologyBuilder.setSpout(spoutId, new ArtificialTweetsEmitterSpout(), 1);
	    topologyBuilder.setBolt(counterId, new TweetAggregateBolt(), 3).fieldsGrouping(spoutId, new Fields("sentiment_id"));
	    topologyBuilder.setBolt(intermediateRankerId, new AnomalyDetectionBolt_PEWA_STDEV(), 3).fieldsGrouping(counterId, new Fields(
	        "sentiment_id"));
	    topologyBuilder.setBolt(summarizeSentimentId, new SummarizeSentimentBolt()).shuffleGrouping(intermediateRankerId);
	    topologyBuilder.setBolt("jmsBolt", jmsBolt).shuffleGrouping(summarizeSentimentId);
	    
		//topologyBuilder.setSpout("wordGenerator", new RandomWordFeeder());
		//topologyBuilder.setBolt("counter", new WordCounterBolt()).shuffleGrouping("wordGenerator");
		//topologyBuilder.setBolt("jmsBolt", jmsBolt).shuffleGrouping("counter");

		final Config config = new Config();
		config.setDebug(false);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-count", config, topologyBuilder.createTopology());
	}
}
