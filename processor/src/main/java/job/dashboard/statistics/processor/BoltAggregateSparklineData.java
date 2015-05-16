package job.dashboard.statistics.processor;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.jdbc.datasource.DriverManagerDataSource;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Lists;

import common.feeder.utility.ApplicationConfigurationFile;
import common.feeder.utility.DatabaseHelper;
import common.feeder.utility.TweetJDBCTemplate;
import data.collection.entity.Queries;
import data.collection.json.JsonDashboardSparkline;

/**
 * 
 * @author Khantil Patel
 * 
 *         TagTweetsWithQueryBolt will process the RAW Tweet and tag them with a
 *         query_id it may belong to. The list of queries is passed from parent
 *         Spout
 *
 *         <<--parallelism Note: We intend to execute 3-executor (3-Tasks by
 *         default)-->>
 */

public class BoltAggregateSparklineData extends BaseBasicBolt {

	private static final long serialVersionUID = 6460656994454296875L;
	String DB_NAME = "meta_db";
	long keywordRefreshInterval = 60000;// 60 sec //300000; // 5min //900000;
	long startTime = 0;

	Queries query;

	static int totalProcessedTweets = 0;
	static int totalEmitedTweets = 0;
	boolean isDebug = false;

	// int AGGREGATION_FACTOR_MINUTES = 3 * 60; // Hours * Minutes

	// int WINDOW_PERIOD = 5 * 24 * 60;

	DriverManagerDataSource datasource;
	TweetJDBCTemplate tweetsJDBCTemplate;
	ApplicationConfigurationFile configFile;
	public static Logger LOG = LoggerFactory
			.getLogger(BoltAggregateSparklineData.class);

	public BoltAggregateSparklineData(ApplicationConfigurationFile _configFile,
			boolean _isDebug) {
		super();

		configFile = _configFile;

		isDebug = _isDebug;

		// datasource = new DriverManagerDataSource();
		// datasource.setDriverClassName("com.mysql.jdbc.Driver");
		// datasource.setUrl(DB_BASE_URL + DB_NAME);// test-replica
		// datasource.setUsername(USERNAME);
		// datasource.setPassword(PASSWORD);

		// tweetsJDBCTemplate.setDataSource(datasource);
	}

	/**
	 * REST API call to sentiment 140
	 */

	@Override
	public void execute(Tuple tuple, BasicOutputCollector collector) {

		List<Object> otherFields = Lists.newArrayList(tuple.getValues());
		query = (Queries) otherFields.get(0);
		tweetsJDBCTemplate = new TweetJDBCTemplate();
		datasource = new DriverManagerDataSource();
		datasource.setDriverClassName("com.mysql.jdbc.Driver");
		datasource.setUrl(configFile.getJdbc_url() + query.getDb_name());// test-replica
		datasource.setUsername(configFile.getJdbc_username());
		datasource.setPassword(configFile.getJdbc_password());

		tweetsJDBCTemplate.setDataSource(datasource);

		JsonDashboardSparkline jsonSparklineObject = DatabaseHelper
				.getAggregatedTweetData(tweetsJDBCTemplate,
						configFile.getJdbc_url(), DB_NAME, query,
						configFile.getSparkline_aggregation_period(),
						configFile.getSparkline_window_period(), LOG);

		String json = "";

		ObjectMapper mapper = new ObjectMapper();
		try {
			json = mapper.writeValueAsString(jsonSparklineObject);
		} catch (JsonProcessingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		if(isDebug){
		LOG.info(json);
		}
		
		collector.emit(new Values(query, json));

	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer ofd) {
		ofd.declare(new Fields("query", "json"));
	}

	/**
	 * Take Raw Tweet Object, convert to TweetTableObject and tag with query
	 * 
	 * @param rawtweetObject
	 */
	// TweetTableObject tagCollectionOfTweets(RawTweet rawtweetObject) {
	//
	//
	// }
}