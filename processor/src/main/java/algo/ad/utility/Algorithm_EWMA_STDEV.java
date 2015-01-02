package algo.ad.utility;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;

import algo.ad.feeder.utility.AggregateUtilityFunctions;

public class Algorithm_EWMA_STDEV {

	// *******CONSTANT VAR's******************************
	private final double DEVIATION_THRESHOLD_LOCAL = 3;

	private final double DEVIATION_THRESHOLD_WINDOW = 3;

	private final int WINDOW_SIZE_MIN = 4320; // 3 days // 8640; // 6 days

	private final boolean WINDOW_MAD_APPROACH = true; // false by default STD_DEV is used
	
	private boolean RETURN_OUTLIER_TYPE_WINDOW = false; // if false by default LocalOutlier is returned
	// ProbabilityEWMA Const's
	double const_alpha_PWMA = 0.99;

	double const_a = 0.3989;
	double const_beta = 1;
	double s1 = 0;
	double s2 = 0;
	// ***************************************************

	// *******CLASS VAR's*********************************
	private double local_mean;
	private double local_std_dev;

	private double window_mean;
	private double window_std_dev;

	public List<Bin> window_list = new ArrayList<Bin>();

	// ********Test Period VAR's***************************
	private final int TEST_PERIOD_MIN = 1440; // 1day
	public List<Bin> test_list = new ArrayList<Bin>();

	private Date test_period_date;

	// ***************************************************

	public boolean find_Outlier(Bin m_Dt, int sentiment) {
		boolean isLocalOutlier = false;
		boolean isWindowOutlier = false;
		Date testDate = null;
		try {
			testDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
					.parse("2013-06-29 15:30:00");
		} catch (ParseException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		if (sentiment == 2 && (testDate.compareTo(m_Dt.getDate()) == 0)
				&& (m_Dt.getCount() == 2667)) {

			System.out.println("debug");
		}

		if (test_period_date == null) {
			test_period_date = AggregateUtilityFunctions.addMinutesToDate(
					TEST_PERIOD_MIN, m_Dt.getDate());
		}

		if (m_Dt.getDate().compareTo(test_period_date) >= 0) {

			double LDF = cal_dev_fact(m_Dt.getCount(), local_mean);
			double test1 = (local_std_dev / local_mean);
			if (LDF > DEVIATION_THRESHOLD_LOCAL * (local_std_dev / local_mean)) {

				isLocalOutlier = true;
				m_Dt.setAnomaly_type(AnomalyType.LOCAL_ANOMALY);
				
				double temp = (Math.abs(m_Dt.getCount() - window_mean))/window_std_dev;
				double WDF = cal_dev_fact(m_Dt.getCount(), window_mean);

//				if (WDF > DEVIATION_THRESHOLD_WINDOW
//						* (window_std_dev / window_mean)) {
				if(temp > DEVIATION_THRESHOLD_WINDOW){
					isWindowOutlier = true;
					m_Dt.setAnomaly_type(AnomalyType.WINDOW_ANOMALY);
					// generate ALERT
				}

				window_list.add(m_Dt);
			}

			updateLocalContext_PEWMA(m_Dt);
			boolean isWindowSlided = slideWindowForward(m_Dt);
			if (isLocalOutlier || isWindowSlided) {
				updateWindowContext();
			}
		} else {
			test_list.add(m_Dt);

			local_mean = cal_mean(test_list);
			local_std_dev = cal_init_mean_dev(test_list);
		}

		boolean finalReturnOutlier = false;
		if(RETURN_OUTLIER_TYPE_WINDOW)
			finalReturnOutlier = isWindowOutlier;
		else
			finalReturnOutlier = isLocalOutlier;
		
		return finalReturnOutlier;

	}

	void updateLocalContext_PEWMA(Bin m_Dt) {
		// 1. Probability function Guassian Distribution
		double z_score = cal_z_score(m_Dt.getCount(), local_mean, local_std_dev);

		// add z-score
		double var1 = -((z_score * z_score) / 2);
		double var2 = Math.exp(var1);
		// 2.
		double probabiltiy = const_a * var2;
		// 3.
		double alfa_t = (1 - const_beta * probabiltiy) * const_alpha_PWMA;

		s1 = alfa_t * s1 + (1 - alfa_t) * m_Dt.getCount();

		s2 = alfa_t * s2 + (1 - alfa_t) * (m_Dt.getCount() * m_Dt.getCount());

		local_mean = s1;
		local_std_dev = Math.sqrt(Math.abs(s2 - (s1 * s1)));
	}

	void updateWindowContext() {
		
		if(WINDOW_MAD_APPROACH)
		calc_window_median_and_MAD();
		else
		calc_window_mean_and_std_dev();
	}

	boolean slideWindowForward(Bin m_Dt) {
		boolean isWindowUpdated = false;
		if (!window_list.isEmpty()) {
			Date window_start_date = AggregateUtilityFunctions
					.minusMinutesToDate(WINDOW_SIZE_MIN, m_Dt.getDate());

			for (Iterator<Bin> iterator = window_list.iterator(); iterator
					.hasNext();) {
				Bin next_bin = iterator.next();
				if (window_start_date.compareTo(next_bin.getDate()) > 0) {
					// Remove the current element from the iterator and the
					// list.
					iterator.remove();
					isWindowUpdated = true;
				} else {
					break;
				}
			}
		}
		return isWindowUpdated;
	}

	// *******MATH FUNCTIONS************************************************
	double cal_dev_fact(double currentValue, double currentMean) {
		double result = 0;
		if (0 != currentMean) {
			double absValue = Math.abs(currentValue - currentMean);
			result = absValue / currentMean;
		}
		return result;
	}

	void calc_window_mean_and_std_dev() {
		double total_dev = 0.0f;
		window_mean = cal_mean(window_list);
		for (Bin bin : window_list) {
			double distance = Math.abs(bin.getCount() - window_mean)
					* Math.abs(bin.getCount() - window_mean);
			total_dev += distance;
		}

		window_std_dev = Math.sqrt(total_dev / window_list.size());

	}

	void calc_window_median_and_MAD() {
		ArrayList<Double> median_list = new ArrayList<Double>();

		ArrayList<Double> mad_list = new ArrayList<Double>();
		for (Bin bin : window_list) {
			median_list.add((double) bin.getCount());
		}

		Collections.sort(median_list);
		window_mean = getMedian(median_list); // Median

		for (Bin bin : window_list) {
			double e = Math.abs(bin.getCount() - window_mean);
			mad_list.add(e);
		}
		
		Collections.sort(mad_list);

		window_std_dev= 1.4826 * getMedian(mad_list); // MAD
	}

	static double cal_mean(List<Bin> bins) {
		double total = 0;
		for (Bin bin : bins) {
			total += bin.getCount();
		}
		return total / bins.size();
	}

	static double cal_init_mean_dev(List<Bin> bins) {
		double total_dev = 0.0f;
		double mean = cal_mean(bins);
		for (Bin bin : bins) {
			double distance = Math.abs(bin.getCount() - mean)
					* Math.abs(bin.getCount() - mean);
			total_dev += distance;
		}

		double result = Math.sqrt(total_dev / bins.size());
		return result;
	}

	static double cal_z_score(double currentValue, double currentMean,
			double currentMeanDev) {
		double result = 0;
		if (0 != currentMeanDev) {
			double absValue = Math.abs(currentValue - currentMean);
			result = absValue / currentMeanDev;
		}
		return result;
	}

	double getMedian(List<Double> median_list) {

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
	// *********************************************************************
}
