package algo.ad.processor;

import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

public class SummarizeSentimentBolt extends BaseRichBolt {

	private static final long serialVersionUID = 5537727428628598519L;
	// private static final Logger LOG =
	// Logger.getLogger(TweetAggregateBolt.class);

	static Integer positive_sentiment_count = null;
	static Integer is_positive_sentiment_anomalous = 0;

	static Integer negative_sentiment_count = null;
	static Integer is_negative_sentiment_anomalous = 0;

	static Integer neutral_sentiment_count = null;
	static Integer is_neutral_sentiment_anomalous = 0;

	static Date current_date = null;
	// private final List<Object> tweetsCounter;
	// private final int aggregateLengthInMinutes;
	private OutputCollector collector;

	public SummarizeSentimentBolt() {

	}

	@SuppressWarnings("rawtypes")
	@Override
	public void prepare(Map stormConf, TopologyContext context,
			OutputCollector collector) {
		this.collector = collector;
		// lastModifiedTracker = new
		// NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
		// this.emitFrequencyInSeconds));
	}

	@Override
	public void execute(Tuple tuple) {
		// if (TupleHelpers.isTickTuple(tuple)) {
		// //
		// LOG.debug("TweetAggregateBolt: Received tick tuple, triggering emit of current aggregate counts");
		// emitCurrentAggregateCounts();
		// } else {
		// countObjAndAck(tuple);
		// }
		List<Object> otherFields = Lists.newArrayList(tuple.getValues());
		int currentCounter = (Integer) otherFields.get(0);
		int currentSentiment = (Integer) otherFields.get(1);
		Date tempDate = (Date) otherFields.get(2); // 0: counter, 1: //
													// sentement_Id, //
													// 2:Date_object
		boolean isAnomalous = (boolean) otherFields.get(3);

		if(isAnomalous)
		{
			System.out.println("Got ya");
		}
		if (tempDate != null) {
			current_date = tempDate;
		}

		if (currentSentiment == 0 && negative_sentiment_count == null) {
			negative_sentiment_count = currentCounter;
			
			if(isAnomalous)
			is_negative_sentiment_anomalous = 1;
		}

		if (currentSentiment == 2 && neutral_sentiment_count == null) {
			neutral_sentiment_count = currentCounter;
			
			if(isAnomalous)
			is_neutral_sentiment_anomalous = 1;
		}

		if (currentSentiment == 4 && positive_sentiment_count == null) {
			positive_sentiment_count = currentCounter;
			
			if(isAnomalous)
			is_positive_sentiment_anomalous = 1;
		}

		if (positive_sentiment_count != null && neutral_sentiment_count != null
				&& negative_sentiment_count != null) {
			
			collector.emit(new Values(positive_sentiment_count,is_positive_sentiment_anomalous,
					negative_sentiment_count, is_negative_sentiment_anomalous,
					neutral_sentiment_count, is_neutral_sentiment_anomalous,
					current_date));

			positive_sentiment_count = null;
			is_positive_sentiment_anomalous = 0;
			negative_sentiment_count = null;
			is_negative_sentiment_anomalous = 0;
			neutral_sentiment_count = null;
			is_neutral_sentiment_anomalous= 0;
		}
	}

	// private void emit(int count, int sentiment, Date emitDate) {
	// // LOG.debug("TweetAggregateBolt: Emit Aggregate, Count:"+count+
	// // "||Sentiment:"+sentiment+"||Timestamp: "+date);
	// System.out.println("TweetAggregateBolt: Emit Aggregate, Count:" + count
	// + "||Sentiment:" + sentiment + "||Timestamp: " + emitDate);
	// currentAggregateCounter = 0;
	// collector.emit(new Values(count, sentiment, emitDate));
	// }

	// private void countObjAndAck(Tuple tuple) {
	//
	//
	// }

	@Override
	public void declareOutputFields(OutputFieldsDeclarer declarer) {
		declarer.declare(new Fields("pos_sentiment_count",
				"pos_sentiment_anomaly", "neg_sentiment_count",
				"neg_sentiment_anomaly", "neu_sentiment_count",
				"neu_sentiment_anomaly", "ObjTimestamp"));
	}

	@Override
	public Map<String, Object> getComponentConfiguration() {
		Map<String, Object> conf = new HashMap<String, Object>();
		// conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_DURATION_SEC);
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
