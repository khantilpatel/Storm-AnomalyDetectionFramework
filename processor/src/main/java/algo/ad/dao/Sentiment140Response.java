package algo.ad.dao;
import com.fasterxml.jackson.annotation.JsonIgnoreProperties;

@JsonIgnoreProperties(ignoreUnknown = true)
public class Sentiment140Response {
	
	String text;
	
	String id;
	
	String query;
	
	String polarity;

	public String getText() {
		return text;
	}

	public void setText(String text) {
		this.text = text;
	}

	public String getId() {
		return id;
	}

	public void setId(String id) {
		this.id = id;
	}

	public String getQuery() {
		return query;
	}

	public void setQuery(String query) {
		this.query = query;
	}

	public String getPolarity() {
		return polarity;
	}

	public void setPolarity(String polarity) {
		this.polarity = polarity;
	} 

}
