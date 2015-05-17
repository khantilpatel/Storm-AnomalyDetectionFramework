package algo.ad.processor;

import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.google.common.collect.Lists;

import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.TweetJDBCTemplate;
import common.feeder.utility.TweetJDBCTemplateConnectionPool;
import data.collection.entity.AnomalyTableObject;
import data.collection.entity.Queries;
import data.collection.entity.TweetTableObject;
import data.collection.entity.TweetTransferEntity;

public class BoltSaveAnomaliesToMySQL extends BaseBasicBolt {

	private static final long serialVersionUID = 6460656994454296875L;

	// *******Custom Ticks****************
	long keywordRefreshInterval = 120000;// 60 sec //300000; // 5min //900000;
											// //15 minute
	long startTime = 0;
	// ***********************************

	boolean isDebug = false;

	int totalSavedTweets = 0;
	int totalProcessedTweets = 0;
	int totalReceivedTweets = 0;

	ApplicationConfigurationFile configFile;

	List<TweetTableObject> listOfTweets;
	public static Logger LOG = LoggerFactory
			.getLogger(BoltSaveAnomaliesToMySQL.class);

	public BoltSaveAnomaliesToMySQL(ApplicationConfigurationFile _configFile,
			boolean _isDebug) {

		configFile = _configFile;

		isDebug = _isDebug;
		listOfTweets = new ArrayList<TweetTableObject>(0);
		startTime = System.currentTimeMillis();
	}

	/**
	 * REST API call to sentiment 140
	 */

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		List<Object> otherFields = Lists.newArrayList(tuple.getValues());
		int currentCounter = (Integer) otherFields.get(0);
		int currentSentiment = (Integer) otherFields.get(1);
		Date tempDate = (Date) otherFields.get(2); // 0: counter, 1: //
													// sentement_Id, //
													// 2:Date_object
		Integer isAnomalous = (Integer) otherFields.get(3);

		List<TweetTransferEntity> tweetList = (List<TweetTransferEntity>) otherFields
				.get(4);

		int aggregation_factor = 15;

		TweetJDBCTemplate tweetJdbcTemplate = TweetJDBCTemplateConnectionPool
				.getTweetJDBCTemplate("test", configFile);

		List<AnomalyTableObject> anomalies = new ArrayList<AnomalyTableObject>(
				0);

		AnomalyTableObject anomaly = new AnomalyTableObject();
		// for (TweetTransferEntity tweetTransferEntity : tweetList) {
		//
		// anomaly.setQuery_id(2);
		// anomaly.setSentiment(currentSentiment);
		// anomaly.setTimestamp(tweetTransferEntity.getTimestamp() / 10000);
		//
		// anomaly.setTweet_id(tweetTransferEntity.getTimestamp());
		// anomaly.setValue(currentCounter);
		// anomaly.setWindow_length(0);
		// anomaly.setAggregation(aggregation_factor);
		// anomaly.setNote(tweettrans);
		//
		// anomalies.add(anomaly);
		// }

		if (isAnomalous == 1 || isAnomalous == 2) {
			anomaly.setQuery_id(2);
			anomaly.setSentiment(currentSentiment);
			anomaly.setTimestamp(tempDate.getTime() / 10000);

			anomaly.setTweet_id(tempDate.getTime());
			anomaly.setValue(currentCounter);
			anomaly.setWindow_length(0);
			anomaly.setAggregation(aggregation_factor);
			anomaly.setNote(tempDate.toString());

			tweetJdbcTemplate.insertAnomalies(anomaly);
		}

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}

	// /Insert Into Database
}
