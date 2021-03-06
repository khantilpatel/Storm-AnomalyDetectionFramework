package algo.ad.processor;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import algo.ad.dao.TweetAggregateBin;
import algo.ad.utility.Algorithm_EWMA_STDEV;
import algo.ad.utility.Bin;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

import data.collection.entity.TweetSentiment;
import data.collection.entity.TweetTransferEntity;

/**
 * This bolt performs rolling counts of incoming objects, i.e. sliding window
 * based counting.
 * <p/>
 * The bolt is configured by two parameters, the length of the sliding window in
 * seconds (which influences the output data of the bolt, i.e. how it will count
 * objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window
 * length is set to an equivalent of five minutes and the emit frequency to one
 * minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * <p/>
 * The bolt emits a rolling count tuple per object, consisting of the object
 * itself, its latest rolling count, and the actual duration of the sliding
 * window. The latter is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high
 * system load. Note that the actual window length is tracked and calculated for
 * the window, and not individually for each object within a window.
 * <p/>
 * Note: During the startup phase you will usually observe that the bolt warns
 * you about the actual sliding window length being smaller than the expected
 * length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during
 * startup (e.g. you will see this warning during the first ~ five minutes of
 * startup time if the window length is set to five minutes).
 */

public class AnomalyDetectionBolt_PEWA_STDEV extends BaseRichBolt {

	// ********Constants**************
	final static int SENTIMENT_LOG = 0;
	// *******************************

	private static final long serialVersionUID = 5537727428628598519L;
	// private static final Logger LOG =
	// Logger.getLogger(TweetAggregateBolt.class);
	// private static final int DEFALUT_AGGREGATE_IN_MINUTES = 360;
	Date nextAggregateDate;
	int currentSentiment;
	// private final List<Object> tweetsCounter;
	private int counter;
	private OutputCollector collector;
	Algorithm_EWMA_STDEV positive_algo_anomalyDetection;
	Algorithm_EWMA_STDEV neutral_algo_anomalyDetection;
	Algorithm_EWMA_STDEV negative_algo_anomalyDetection;

	

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		negative_algo_anomalyDetection = new Algorithm_EWMA_STDEV(
				TweetSentiment.NEGATIVE, "out_negative");
		neutral_algo_anomalyDetection = new Algorithm_EWMA_STDEV(
				TweetSentiment.NEUTRAL, "out_neutral");
		positive_algo_anomalyDetection = new Algorithm_EWMA_STDEV(
				TweetSentiment.POSITIVE, "out_positive");

		// lastModifiedTracker = new
		// NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
		// this.emitFrequencyInSeconds));
	}

	@Override
	public void execute(Tuple tuple) {

		countObjAndAck(tuple);
	}

	private void emit(int count, int sentiment, Date emitDate) {
		Date date = new java.util.Date();
	
		if (sentiment == SENTIMENT_LOG) {
//			System.out.println("TweetAggregateBolt: Emit Aggregate, Count:"
//					+ count + "||Sentiment:" + sentiment + "||Timestamp: "
//					+ date);
		}
		counter = 0;
		// collector.emit(new Values(count, date));
	}

	private void countObjAndAck(Tuple tuple) {
		
		long startTime = System.currentTimeMillis();
		int isAnomaly = 0;
		List<Object> otherFields = Lists.newArrayList(tuple.getValues());
//		int currentCounter = (Integer) otherFields.get(0);
		currentSentiment = (Integer) otherFields.get(1);
//		Date currentDate = (Date) otherFields.get(2); // 0: counter, 1:
//														// sentement_Id,
//														// 2:Date_object
//
//		 Map<Long,Long> tweetMap = (Map<Long,Long>) otherFields.get(3);
//		 
		TweetAggregateBin bin = (TweetAggregateBin) otherFields.get(3);
		
		Bin currentBin = new Bin();

		currentBin.setCount(bin.getCounter());
		currentBin.setDate(bin.getDate());
		
		List<TweetTransferEntity> tweetList = bin.getTweetList();
		String str_tweet_ids = "";
		for (TweetTransferEntity tweetTransferEntity : tweetList) {
			str_tweet_ids += String.valueOf(tweetTransferEntity.getTimestamp());
		}
		

		if (currentSentiment == SENTIMENT_LOG) {
		 System.out.println("AnomalyDetectionBolt_PEWA_STDEV: Emit Aggregate, Count:"+bin.getCounter()+
		 "||Sentiment:"+currentSentiment+"||Timestamp: "+ bin.getDate()+"||tweetListCount::" +bin.getTweetList().size()
		 +"|| list::"+str_tweet_ids);
		}

		Date check_date = null;
		try {
			check_date = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
			.parse("2015-09-15 10:00:00");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		
		if(currentBin.getDate().after(check_date) && currentSentiment == SENTIMENT_LOG)
		{
			System.out.println();
		}
	
		// Negative ==0
		if (currentSentiment == 0) {

			isAnomaly = negative_algo_anomalyDetection.find_Outlier(currentBin,
					currentSentiment);
		}

		// Neutral == 2
		if (currentSentiment == 2) {
			isAnomaly = neutral_algo_anomalyDetection.find_Outlier(currentBin,
					currentSentiment);
		}

		// Positive == 4
		if (currentSentiment == 4) {
			isAnomaly = positive_algo_anomalyDetection.find_Outlier(currentBin,
					currentSentiment);
		}

		// algo_anomalyDetection.update_window(currentBin);
		//
		// isAnomaly =
		// algo_anomalyDetection.find_peak_window(currentBin,currentSentiment);

		// int index = 0;
		// if(currentSentiment == SENTIMENT_LOG){
		// System.out
		// .println("\n ***********Peak Windows In the List for sentiment "
		// + currentSentiment + "||count:"+
		// algo_anomalyDetection.peak_Windows.size()+ "***********");
		// System.out.println("Processing: time:" + currentDate + "|| count:"+
		// currentCounter +
		// "|| isAnomaly:" + isAnomaly);
		//
		// for (PickWindow window : algo_anomalyDetection.peak_Windows) {
		// index++;
		// System.out.println(index +"||"+currentDate+
		// "||Sentiment:"+currentSentiment +"||StartTime:" +
		// window.getStartTime()
		// + "||EndTime:" + window.getEndTime()+ "||Max:"+window.getMaxValue()
		// );
		//
		// System.out.println("\n");
		// }
		// }
		if (isAnomaly > 0) {
			// System.out.println("Got ya");
		}
		long elapsedTime = System.currentTimeMillis() - startTime;
		
		if (currentSentiment == SENTIMENT_LOG) {
			 System.out.println("AnomalyDetectionBolt_PEWA_STDEV: execution Time::"+elapsedTime/1000 +" sec || Emit Aggregate, Count:"+bin.getCounter()+
			 "||Sentiment:"+currentSentiment+"||Timestamp: "+ bin.getDate()+"||tweetListCount::" +bin.getTweetList().size()
			 +"|| list::"+str_tweet_ids);
			}
		collector.emit(new Values(bin.getCounter(), currentSentiment,
				bin.getDate(), isAnomaly, bin.getTweetList()));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("count", "sentiment_id", "ObjTimestamp",
				"isAnomaly", "tweetList"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		// conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS,
		// aggregateLengthInSeconds);
		return conf;
	}

	// private void emitCurrentWindowCounts() {
	// // Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
	// // int actualWindowLengthInSeconds =
	// lastModifiedTracker.secondsSinceOldestModification();
	// // lastModifiedTracker.markAsModified();
	// // if (actualWindowLengthInSeconds != windowLengthInSeconds) {
	// // LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE,
	// actualWindowLengthInSeconds, windowLengthInSeconds));
	// // }
	//
	//
	// // emit(counter);
	// }
}
