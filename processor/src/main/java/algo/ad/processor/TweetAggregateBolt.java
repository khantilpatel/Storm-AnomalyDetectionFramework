package algo.ad.processor;


import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TimeZone;

import algo.ad.feeder.utility.AggregateUtilityFunctions;
import algo.ad.feeder.utility.TupleHelpers;
import backtype.storm.Config;
import backtype.storm.task.OutputCollector;
import backtype.storm.task.TopologyContext;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseRichBolt;
import backtype.storm.tuple.Fields;
import backtype.storm.tuple.Tuple;
import backtype.storm.tuple.Values;

import com.google.common.collect.Lists;

/**
 * This bolt performs rolling counts of incoming objects, i.e. sliding window based counting.
 * <p/>
 * The bolt is configured by two parameters, the length of the sliding window in seconds (which influences the output
 * data of the bolt, i.e. how it will count objects) and the emit frequency in seconds (which influences how often the
 * bolt will output the latest window counts). For instance, if the window length is set to an equivalent of five
 * minutes and the emit frequency to one minute, then the bolt will output the latest five-minute sliding window every
 * minute.
 * <p/>
 * The bolt emits a rolling count tuple per object, consisting of the object itself, its latest rolling count, and the
 * actual duration of the sliding window. The latter is included in case the expected sliding window length (as
 * configured by the user) is different from the actual length, e.g. due to high system load. Note that the actual
 * window length is tracked and calculated for the window, and not individually for each object within a window.
 * <p/>
 * Note: During the startup phase you will usually observe that the bolt warns you about the actual sliding window
 * length being smaller than the expected length. This behavior is expected and is caused by the way the sliding window
 * counts are initially "loaded up". You can safely ignore this warning during startup (e.g. you will see this warning
 * during the first ~ five minutes of startup time if the window length is set to five minutes).
 */

public class TweetAggregateBolt extends BaseRichBolt {

	//********Constants**************
	final static int SENTIMENT_LOG = 2;
	private static final int AGGREGATION_FACTOR_MINUTES = 15;//360; //15
	//final int TICK_DURATION_SEC = 1;
	//*******************************
	
  private static final long serialVersionUID = 5537727428628598519L;
 // private static final Logger LOG = Logger.getLogger(TweetAggregateBolt.class);


   Date currentAggregateDate;
  int currentSentiment;
  private int currentAggregateCounter;
  //private final List<Object> tweetsCounter;  
  //private final int aggregateLengthInMinutes;
  private OutputCollector collector;

  public TweetAggregateBolt() {

  }

 

  @SuppressWarnings("rawtypes")
  @Override
  public void prepare(Map stormConf, TopologyContext context, OutputCollector collector) {
    this.collector = collector;
   // lastModifiedTracker = new NthLastModifiedTimeTracker(deriveNumWindowChunksFrom(this.windowLengthInSeconds,
     //   this.emitFrequencyInSeconds));
  }

  @Override
  public void execute(Tuple tuple) {
	//  System.out.println("TAB:Default TimeZone:"+TimeZone.getDefault());
	  List<Object> otherFields = Lists.newArrayList(tuple.getValues());
	  int tupleType= (Integer) otherFields.get(4);
	  
    if (0 == tupleType){//TupleHelpers.isTickTuple(tuple)) {
     // LOG.debug("TweetAggregateBolt: Received tick tuple, triggering emit of current aggregate counts");
      emitCurrentAggregateCounts();
    }
    else {
        countObjAndAck(tuple);  
    }
  }


  private void emit(int count,int sentiment, Date emitDate) {
	 // LOG.debug("TweetAggregateBolt: Emit Aggregate, Count:"+count+ "||Sentiment:"+sentiment+"||Timestamp: "+date);
	  if(sentiment == SENTIMENT_LOG){
		//  System.out.println("TweetAggregateBolt: Emit Aggregate, Count:"+count+ "||Sentiment:"+sentiment+"||Timestamp: "+emitDate);
	  }
	  currentAggregateCounter = 0;
      collector.emit(new Values(count, sentiment, emitDate));
  }

  private void emitCurrentAggregateCounts()
  {
	  if(currentAggregateCounter == 0 && currentAggregateDate != null)
	  {
		  currentAggregateDate =  AggregateUtilityFunctions.addMinutesToDate(
					AGGREGATION_FACTOR_MINUTES, currentAggregateDate);
	  }
	  this.emit(currentAggregateCounter, currentSentiment, currentAggregateDate);
  }
  
  private void countObjAndAck(Tuple tuple) {
	  
		///////////////////////////////////////////////////////////////////////////////////////////
		// COUNT THE TWEETS HERE AND EMIT THEM WHEN TICK IS RECIEVED
		  List<Object> otherFields = Lists.newArrayList(tuple.getValues());
		  currentSentiment = (Integer) otherFields.get(1);
		  //currentAggregateDate = (Date) otherFields.get(2); // 0: tweet_Id, 1: sentement_Id, 2:Date_object
		  currentAggregateDate = (Date) otherFields.get(3);
		  currentAggregateCounter++;
		  
		  if(currentAggregateDate == null){
			  if(currentSentiment == SENTIMENT_LOG){
			  System.out.println("Wait");
			  }
		  }
		///////////////////////////////////////////////////////////////////////////////////////////
		  
	  ////OLD CODE FOR AGGREGATION////////////////////////////////////////////////////////////////
			/* List<Object> otherFields = Lists.newArrayList(tuple.getValues());
			  Date currentDate = (Date) otherFields.get(2); // 0: tweet_Id, 1: sentement_Id, 2:Date_object
			  currentSentiment = (Integer) otherFields.get(1);
			  if(nextAggregateDate == null)
			  {
				  nextAggregateDate =   AggregateUtilityFunctions
					.addMinutesToDate(aggregateLengthInMinutes, currentDate);
				  counter++;
				  System.out.println("TweetAggregateBolt: Count: "+counter+" ||"+tuple);
				  collector.ack(tuple);
			  }
			  else{
				  if(currentDate.compareTo(nextAggregateDate) < 0){
					  counter++;
					  System.out.println("TweetAggregateBolt++: Count: "+counter+" ||"+tuple);
					  collector.ack(tuple);
				  }
				  else
				  {
					  this.emit(counter, currentSentiment, nextAggregateDate);
					  nextAggregateDate =  AggregateUtilityFunctions
						.addMinutesToDate(aggregateLengthInMinutes, nextAggregateDate);
					  counter = 1;			  
				  }
			  }*/
	  //////////////////////////////////////////////////////////////////////////////////////////
  }

  @Override
  public void declareOutputFields(OutputFieldsDeclarer declarer) {
    declarer.declare(new Fields( "count", "sentiment_id", "ObjTimestamp"));
  }

  @Override
  public Map<String, Object> getComponentConfiguration() {
    Map<String, Object> conf = new HashMap<String, Object>();
   // conf.put(Config.TOPOLOGY_TICK_TUPLE_FREQ_SECS, TICK_DURATION_SEC);
    return conf;
  }
  

//private void emitCurrentWindowCounts() {
////  Map<Object, Long> counts = counter.getCountsThenAdvanceWindow();
////  int actualWindowLengthInSeconds = lastModifiedTracker.secondsSinceOldestModification();
////  lastModifiedTracker.markAsModified();
////  if (actualWindowLengthInSeconds != windowLengthInSeconds) {
////    LOG.warn(String.format(WINDOW_LENGTH_WARNING_TEMPLATE, actualWindowLengthInSeconds, windowLengthInSeconds));
////  }
//	  
//	  
//// emit(counter);
//}
}
