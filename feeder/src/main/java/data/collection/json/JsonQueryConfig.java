package data.collection.json;


public class JsonQueryConfig {

	boolean isAnomalyEnabled;

	int deviationThresholdLocal;

	int deviationThresholdWindow;

	public boolean isAnomalyEnabled() {
		return isAnomalyEnabled;
	}

	public void setAnomalyEnabled(boolean isAnomalyEnabled) {
		this.isAnomalyEnabled = isAnomalyEnabled;
	}

	public int getDeviationThresholdLocal() {
		return deviationThresholdLocal;
	}

	public void setDeviationThresholdLocal(int deviationThresholdLocal) {
		this.deviationThresholdLocal = deviationThresholdLocal;
	}

	public int getDeviationThresholdWindow() {
		return deviationThresholdWindow;
	}

	public void setDeviationThresholdWindow(int deviationThresholdWindow) {
		this.deviationThresholdWindow = deviationThresholdWindow;
	}
	
	
	
	
}
