package data.collection.json;

public class JsonAnomalyConfig {

	//{"window_period": {"value": 1,"type": "month"},
	//"aggregation":{"value":3,"type":"hours"},
	//"window_threshold": 3,"local_threshold": 3}
	
	JsonAnomalyConfigTimeValues window_period;
	
	JsonAnomalyConfigTimeValues aggregation;
	
	int window_threshold;
	
	int local_threshold;

	public JsonAnomalyConfigTimeValues getWindow_period() {
		return window_period;
	}

	public void setWindow_period(JsonAnomalyConfigTimeValues window_period) {
		this.window_period = window_period;
	}

	public JsonAnomalyConfigTimeValues getAggregation() {
		return aggregation;
	}

	public void setAggregation(JsonAnomalyConfigTimeValues aggregation) {
		this.aggregation = aggregation;
	}

	public int getWindow_threshold() {
		return window_threshold;
	}

	public void setWindow_threshold(int window_threshold) {
		this.window_threshold = window_threshold;
	}

	public int getLocal_threshold() {
		return local_threshold;
	}

	public void setLocal_threshold(int local_threshold) {
		this.local_threshold= local_threshold;
	}
	
}
