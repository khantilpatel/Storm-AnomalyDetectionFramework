package algo.ad.runner;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

public class TestMAD {

	public static void main(String[] args) {
		// TODO Auto-generated method stub
		double window_mean= 0;
		double window_std_dev = 0;
		
		Double array1[] = {1.0, 4.0, 4.0, 4.0, 5.0, 5.0, 5.0, 5.0, 7.0, 7.0, 8.0, 10.0, 16.0, 30.0};
		List<Double> median_list = new ArrayList<Double>() ;
		List<Double> window_list = Arrays.asList(array1);
		ArrayList<Double> mad_list = new ArrayList<Double>();
		for (Double bin : window_list) {
			median_list.add((double) bin);
		}

		Collections.sort(median_list);
		window_mean = getMedian(median_list); // Median

		for (Double bin : window_list) {
			double e = Math.abs(bin- window_mean);
			mad_list.add(e);
		}

		Collections.sort(mad_list);

		double mad_median = getMedian(mad_list);
		
		window_std_dev = 1.4826 * mad_median; // MAD
		
		System.out.println("Med"+ window_mean);
		System.out.println("MAD" + window_std_dev);
	}
	
	static double getMedian(List<Double> median_list) {

		double median = 0;
		double pos1 = Math.floor((median_list.size() - 1.0) / 2.0);
		double pos2 = Math.ceil((median_list.size() - 1.0) / 2.0);
		if (pos1 == pos2) {
			median = median_list.get((int) pos1);
		} else {
			median = (median_list.get((int) pos1) + median_list.get((int) pos2)) / 2.0;
		}
		return median;
	}

}
