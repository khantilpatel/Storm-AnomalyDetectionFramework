package job.dashboard.statistics.processor;

import java.util.ArrayList;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import algo.ad.processor.tweets.BoltSaveTweetToMySQL;
import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.TweetJDBCTemplate;
import common.feeder.utility.TweetJDBCTemplateConnectionPool;
import data.collection.entity.Queries;
import data.collection.entity.TweetSentiment;
import data.collection.entity.TweetSentimentCountEntity;
import data.collection.json.JsonTweetCounterEntity;

public class BoltSaveSparklineJSONToMySQL extends BaseBasicBolt {

	private static final long serialVersionUID = 6460656994454296875L;

	String DB_NAME = "meta_db";

	boolean isDebug = false;

	int totalSavedTweets = 0;
	int totalProcessedTweets = 0;
	int totalReceivedTweets = 0;

	DriverManagerDataSource datasource;
	TweetJDBCTemplate tweetsJDBCTemplate;
	ApplicationConfigurationFile configFile;
	public static Logger LOG = LoggerFactory
			.getLogger(BoltSaveTweetToMySQL.class);

	public BoltSaveSparklineJSONToMySQL(
			ApplicationConfigurationFile _configFile, boolean _isDebug) {
		super();
		configFile = _configFile;
		isDebug = _isDebug;
	}

	/**
	 * REST API call to sentiment 140
	 */

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		List<Object> otherFields = Lists.newArrayList(tuple.getValues());

		Queries query = (Queries) otherFields.get(0);
		String json = (String) otherFields.get(1);
		String counterJson = "";
		List<TweetSentimentCountEntity> listTweetCountBySentiment = new ArrayList<TweetSentimentCountEntity>(
				0);

		TweetJDBCTemplate query_TweetJDBCTemplate = TweetJDBCTemplateConnectionPool
				.getTweetJDBCTemplate(query.getDb_name(), configFile);

		listTweetCountBySentiment = query_TweetJDBCTemplate
				.listTweetCountBySentiment("SELECT sentiment, COUNT(1) AS tweet_count FROM tweets WHERE query_id = "
						+ query.getQuery_id() + " GROUP BY sentiment;");

		JsonTweetCounterEntity counterEntity = new JsonTweetCounterEntity();

		for (TweetSentimentCountEntity sentimentCount : listTweetCountBySentiment) {
			// queries.setDb_name(db_name);
			if (sentimentCount.getSentiment() == TweetSentiment.NEGATIVE) {
				counterEntity.setNegativeTweetCount(sentimentCount.getCount());
			} else if (sentimentCount.getSentiment() == TweetSentiment.NEUTRAL) {
				counterEntity.setNeutralTweetCount(sentimentCount.getCount());
			} else if (sentimentCount.getSentiment() == TweetSentiment.POSITIVE) {
				counterEntity.setPositiveTweetCount(sentimentCount.getCount());
			}
		}

		counterEntity.setTotal(counterEntity.getNegativeTweetCount()
				+ counterEntity.getNeutralTweetCount()
				+ counterEntity.getPositiveTweetCount());

		ObjectMapper mapper = new ObjectMapper();
		try {
			counterJson = mapper.writeValueAsString(counterEntity);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if (isDebug) {
			LOG.info(json);
		}

		query_TweetJDBCTemplate.updateQueryForSparklineJSON(query, counterJson,
				json);

		totalReceivedTweets++;
		totalProcessedTweets++;
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
	}

}
