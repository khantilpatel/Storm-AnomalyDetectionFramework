package data.collection.json;

import java.util.ArrayList;

public class JsonSparklineObject {

	int sentimentId;
	
	ArrayList<JsonBin> data ;
	
	

	public JsonSparklineObject(int sentiment) {
		sentimentId = sentiment;
		data = new ArrayList<JsonBin>();
	}

	public int getSentimentId() {
		return sentimentId;
	}

	public void setSentimentId(int sentimentId) {
		this.sentimentId = sentimentId;
	}

	public ArrayList<JsonBin> getData() {
		return data;
	}

	public void setData(ArrayList<JsonBin> data) {
		this.data = data;
	}
	
	public void addToData(JsonBin bin) {
		this.data.add(bin);
	}
	
	
}
