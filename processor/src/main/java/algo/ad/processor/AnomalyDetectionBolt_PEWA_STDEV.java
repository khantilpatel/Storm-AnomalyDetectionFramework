package algo.ad.processor;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import algo.ad.utility.Algorithm_EWMA_STDEV;
import algo.ad.utility.Algorithm_Peak_Windows;
import algo.ad.utility.Bin;
import algo.ad.utility.PickWindow;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

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

	//********Constants**************
	final static int SENTIMENT_LOG = 2;	
	//*******************************
	
	
	private static final long serialVersionUID = 5537727428628598519L;
	// private static final Logger LOG =
	// Logger.getLogger(TweetAggregateBolt.class);
	//private static final int DEFALUT_AGGREGATE_IN_MINUTES = 360;
	Date nextAggregateDate;
	int currentSentiment;
	// private final List<Object> tweetsCounter;
	private int counter;
	private OutputCollector collector;
	Algorithm_EWMA_STDEV algo_anomalyDetection;

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		algo_anomalyDetection = new Algorithm_EWMA_STDEV();
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
		// LOG.debug("TweetAggregateBolt: Emit Aggregate, Count:"+count+
		// "||Sentiment:"+sentiment+"||Timestamp: "+date);
		if(sentiment ==  SENTIMENT_LOG){
		System.out.println("TweetAggregateBolt: Emit Aggregate, Count:" + count
				+ "||Sentiment:" + sentiment + "||Timestamp: " + date);
		}
		counter = 0;
		// collector.emit(new Values(count, date));
	}

	private void countObjAndAck(Tuple tuple) {
		boolean isAnomaly = false;
		List<Object> otherFields = Lists.newArrayList(tuple.getValues());
		int currentCounter = (Integer) otherFields.get(0);
		currentSentiment = (Integer) otherFields.get(1);
		Date currentDate = (Date) otherFields.get(2); // 0: counter, 1:
														// sentement_Id,
														// 2:Date_object

		Bin currentBin = new Bin();

		currentBin.setCount(currentCounter);
		currentBin.setDate(currentDate);
		
		isAnomaly = algo_anomalyDetection.find_Outlier(currentBin, currentSentiment);
		
//		algo_anomalyDetection.update_window(currentBin);
//		
//		isAnomaly = algo_anomalyDetection.find_peak_window(currentBin,currentSentiment);

//		int index = 0;
//		if(currentSentiment ==  SENTIMENT_LOG){
//			System.out
//					.println("\n ***********Peak Windows In the List for sentiment "
//							+ currentSentiment + "||count:"+ 
//							algo_anomalyDetection.peak_Windows.size()+ "***********");
//			System.out.println("Processing: time:" + currentDate + "|| count:"+ currentCounter + 
//					"|| isAnomaly:" + isAnomaly);
//			
//			for (PickWindow window : algo_anomalyDetection.peak_Windows) {
//				index++;
//				System.out.println(index +"||"+currentDate+ "||Sentiment:"+currentSentiment +"||StartTime:" + window.getStartTime()
//						+ "||EndTime:" + window.getEndTime()+ "||Max:"+window.getMaxValue() );
//			
//				System.out.println("\n");
//			}
//		}
		if(isAnomaly)
		{
			System.out.println("Got ya");
		}
		collector.emit(new Values(currentCounter, currentSentiment,
				currentDate, isAnomaly));
	}

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("count", "sentiment_id", "ObjTimestamp",
				"isAnomaly"));
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
