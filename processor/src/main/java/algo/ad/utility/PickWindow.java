package algo.ad.utility;

import java.util.Date;

public class PickWindow {
	
	protected Date startTime;
	protected Date endTime;
	protected int maxValue;
	protected int minValue;
	protected float meanValue;
	protected int beanCount;
	protected float meanDev;
	
	public Date getStartTime() {
		return startTime;
	}
	public void setStartTime(Date startTime) {
		this.startTime = startTime;
	}
	public Date getEndTime() {
		return endTime;
	}
	public void setEndTime(Date endTime) {
		this.endTime = endTime;
	}
	public int getMaxValue() {
		return maxValue;
	}
	public void setMaxValue(int maxValue) {
		this.maxValue = maxValue;
	}
	public int getMinValue() {
		return minValue;
	}
	public void setMinValue(int minValue) {
		this.minValue = minValue;
	}
	public float getMeanValue() {
		return meanValue;
	}
	public void setMeanValue(float meanValue) {
		this.meanValue = meanValue;
	}
	public int getBeanCount() {
		return beanCount;
	}
	public void setBeanCount(int beanCount) {
		this.beanCount = beanCount;
	}
	public float getMeanDev() {
		return meanDev;
	}
	public void setMeanDev(float meanDev) {
		this.meanDev = meanDev;
	}
	
	
	

}
