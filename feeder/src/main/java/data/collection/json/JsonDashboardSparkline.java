package data.collection.json;

import java.util.HashMap;

public class JsonDashboardSparkline {
	
	HashMap<Integer,JsonSparklineObject> line;

	public JsonDashboardSparkline() {
		line = new HashMap<Integer,JsonSparklineObject>(4);
		line.put(new Integer(2), new JsonSparklineObject(2));
		line.put(new Integer(4), new JsonSparklineObject(4));
		line.put(new Integer(0), new JsonSparklineObject(0));
		
		JsonSparklineObject object = line.get(0);
		System.out.println(""+object);
	}

	public HashMap<Integer,JsonSparklineObject> getLine() {
		return line;
	}

	public void setLine(HashMap<Integer,JsonSparklineObject> line) {
		this.line = line;
	}

}
