package algo.ad.dao;

import data.collection.entity.TweetSentiment;

public class AggregateTweetsWithSentimentDAO {

	int positiveSentiment;
	
	int neutralSentiment;
	
	int negativeSentiment;
	
	public void incrementCounter(TweetSentiment sentiment)
	{
		if(sentiment == TweetSentiment.POSITIVE)
		{
			positiveSentiment++;
		}
		else if(sentiment == TweetSentiment.NEUTRAL)
		{
			neutralSentiment++;
		}else if(sentiment == TweetSentiment.NEGATIVE)
		{
			negativeSentiment++;
		}
	}
	
	public int getCounter(TweetSentiment sentiment)
	{
		int counter = 0;
		
		if(sentiment == TweetSentiment.POSITIVE)
		{
			counter = positiveSentiment;
		}
		else if(sentiment == TweetSentiment.NEUTRAL)
		{
			counter = neutralSentiment;
		}else if(sentiment == TweetSentiment.NEGATIVE)
		{
			counter = negativeSentiment;
		}
		
		return counter;
	}
	
}
