package algo.ad.processor;

import java.util.List;

import com.google.common.collect.Lists;

import backtype.storm.topology.BasicOutputCollector;
import backtype.storm.topology.OutputFieldsDeclarer;
import backtype.storm.topology.base.BaseBasicBolt;
import backtype.storm.tuple.Tuple;
import twitter4j.Status;


public class PrinterBolt extends BaseBasicBolt {

	  /**
	 * 
	 */
	private static final long serialVersionUID = -7393497899130226755L;

	@Override
	  public void execute(Tuple tuple, BasicOutputCollector collector) {
		 List<Object> otherFields = Lists.newArrayList(tuple.getValues());
		  Status tweetObject= (Status) otherFields.get(0);
	    System.out.println(tweetObject.getText());
	  }

	  @Override
	  public void declareOutputFields(OutputFieldsDeclarer ofd) {
	  }

	}