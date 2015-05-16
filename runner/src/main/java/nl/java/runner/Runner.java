/*package nl.java.runner;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import javax.jms.JMSException;
import javax.jms.Message;
import javax.jms.Session;

import nl.avisi.feeder.RandomWordFeeder;
import nl.avisi.jms.SpringJmsProvider;
import nl.avisi.processor.WordCounterBolt;

import org.apache.xbean.spring.context.ClassPathXmlApplicationContext;
import org.springframework.context.ApplicationContext;

import backtype.storm.Config;
import backtype.storm.LocalCluster;
import backtype.storm.contrib.jms.JmsMessageProducer;
import backtype.storm.contrib.jms.JmsProvider;
import backtype.storm.contrib.jms.bolt.JmsBolt;
import backtype.storm.topology.TopologyBuilder;
import backtype.storm.tuple.Tuple;

*//**
 * @author robbreuk
 * @author Prashanth
 *//*
public final class Runner {
	public static final void main(final String[] args) throws Exception {
		final ApplicationContext applicationContext = new ClassPathXmlApplicationContext("applicationContext.xml");

		final JmsProvider jmsQueueProvider = new SpringJmsProvider(applicationContext, "jmsConnectionFactory",
				                                                          "notificationQueue");

		final TopologyBuilder topologyBuilder = new TopologyBuilder();

		final JmsBolt jmsBolt = new JmsBolt();
		jmsBolt.setJmsProvider(jmsQueueProvider);
		jmsBolt.setJmsMessageProducer(new JmsMessageProducer() {
			@Override
			public final Message toMessage(final Session session, final Tuple input) throws JMSException {
				//final String json = "{\"word\":\"" + input.getString(0) + "\", \"count\":" + String.valueOf(input.getInteger(1)) + "}";
				DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
				Date date = new Date();
			
				final String json = 
						"{\"x\":\"" + dateFormat.format(date) + 
						"\", \"y0\":\"" + String.valueOf(input.getInteger(1)) + 
						"\", \"y2\":\"" + String.valueOf(input.getInteger(1)) + 
						"\", \"y4\":\"" + String.valueOf(input.getInteger(1)) +
						"\"}";
				
				System.out.println(json+"\n");
				return session.createTextMessage(json);
			}
		});

		topologyBuilder.setSpout("wordGenerator", new RandomWordFeeder());
		topologyBuilder.setBolt("counter", new WordCounterBolt()).shuffleGrouping("wordGenerator");
		topologyBuilder.setBolt("jmsBolt", jmsBolt).shuffleGrouping("counter");

		final Config config = new Config();
		config.setDebug(false);

		final LocalCluster cluster = new LocalCluster();
		cluster.submitTopology("word-count", config, topologyBuilder.createTopology());
	}
}
*/