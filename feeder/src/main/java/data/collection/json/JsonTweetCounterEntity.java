package data.collection.json;

public class JsonTweetCounterEntity {

	long total = 0;
	
	long negativeTweetCount = 0;
	
	long positiveTweetCount = 0;
	
	long neutralTweetCount = 0;

	public long getTotal() {
		return total;
	}

	public void setTotal(long total) {
		this.total = total;
	}

	public long getNegativeTweetCount() {
		return negativeTweetCount;
	}

	public void setNegativeTweetCount(long negativeTweetCount) {
		this.negativeTweetCount = negativeTweetCount;
	}

	public long getPositiveTweetCount() {
		return positiveTweetCount;
	}

	public void setPositiveTweetCount(long positiveTweetCount) {
		this.positiveTweetCount = positiveTweetCount;
	}

	public long getNeutralTweetCount() {
		return neutralTweetCount;
	}

	public void setNeutralTweetCount(long neutralTweetCount) {
		this.neutralTweetCount = neutralTweetCount;
	}
	
}
