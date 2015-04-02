package algo.ad.utility;

import java.io.FileWriter;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Date;
import java.util.Iterator;
import java.util.List;
import java.util.TimeZone;

import algo.ad.feeder.dao.TweetSentiment;
import algo.ad.feeder.utility.AggregateUtilityFunctions;

public class Algorithm_EWMA_STDEV {

	TweetSentiment current_sentiment;
	boolean LOGGING = true;
	Date log_date_start;
	Date log_date_end;
	// *******CONSTANT
	// VAR's*********************************************************

	private final int WINDOW_SIZE_MIN = 8640;// 4320: 3 days // 8640: 6 days
	// 1 Day = 1440 ; 2 Day = 2880 ; 3 days = 4320; 6 days = 8640;
	// ******** LOCAL OUTLIER DETECTION APPROACH*************************
	private boolean USE_PEWMA = true; // if false by default use PEWMA
	private final boolean WINDOW_MAD_APPROACH = true; // if false by default//

	private final double DEVIATION_THRESHOLD_LOCAL = 4;
	private final double DEVIATION_THRESHOLD_WINDOW = 4;

	// ******** WINDOW DETECTION APPROACH*************************

	// *****************************************************************************

	// *******************DISPLAY OUTLIER TYPE*************************
	private boolean RETURN_OUTLIER_TYPE_WINDOW = true; // if false by default
														// show local outlier
	private boolean SHOW_INNOVATIVE_OUTLIERS = false; // if false by default
														// Addative outliers are
														// shown
	SimpleDateFormat dateFormat;
	
	
	// LocalOutlier is
	// returned

	// ProbabilityEWMA Const's
	double const_alpha_PEWMA = 0.99;// 0.97;

	double const_alpha_EWMA = 0.97;// 0.875; // 1-0.125;

	double const_a = 0.3989;

	double const_beta = 1;
	double s1 = 0;
	double s2 = 0;
	// ***************************************************

	// *******CLASS VAR's*********************************

	private double local_mean; // PEWMA
	private double local_std_dev; // PEWMA_STD

	private double window_mean; // MAD_median
	private double window_std_dev; // MAD

	// ******Logging Variables****************************************
	FileWriter fw;
	FileWriter fw_csv;
	// FileWriter fw_positive;

	// private double log_local_mean_1; // EWMA
	// private double log_local_std_dev_1; // EWMA_STD
	//
	// private double log_window_mean_1; // STD_mean
	// private double log_window_std_dev_1; // STD

	// Positive
	private double log_EWMA;
	private double log_EWMA_STD;
	private double log_PEWMA;
	private double log_PEWMA_STD;
	private double log_STD_mean;
	private double log_STD;
	private double log_MAD_median;
	private double log_MAD;
	double log_s1 = 0;
	double log_s2 = 0;
	// ***********************************************************

	public List<Bin> window_list = new ArrayList<Bin>();

	// ********Test Period VAR's***************************
	private final int TEST_PERIOD_MIN = 1440; // 1day
	public List<Bin> test_list = new ArrayList<Bin>();

	private Date test_period_date;

	// ***************************************************

	// **********Evaluation Results************************
	Double array_references[] = { 33.4, 434.4 };

	public List<Double> predictions = new ArrayList<Double>();
	public List<Double> references = Arrays.asList(array_references);

	// ****************************************************

	boolean flag_Addative = false;

	public Algorithm_EWMA_STDEV(TweetSentiment m_sentiment, String m_logFileName) {
		super();
		try {
			dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			dateFormat.setTimeZone(TimeZone.getTimeZone("GMT"));
			
			fw = new FileWriter(m_logFileName+".log");
			fw_csv = new FileWriter(m_logFileName+".csv");
			
			// Write header to csv
			String delimiter = ",";
			fw_csv.write(
					"date" + delimiter
					+ "tweets" + delimiter 
					+ "candidate_anomaly" + delimiter 
					+ "legitimate_anomaly" + delimiter 
					+ "EWMA" + delimiter 
					+ "EWMA_STD" +delimiter
					+ "EWMA_STD_minus" +delimiter
					+ "PEWMA" + delimiter
					+ "PEWMA_STD" + delimiter
					+ "PEWMA_STD_minus" + delimiter
					+ "window_STD_mean"  + delimiter 
					+ "window_STD" +delimiter 
					+ "window_MAD_median" +delimiter 
					+ "window_MAD"		
					+ System.getProperty("line.separator"));
			
			current_sentiment = m_sentiment;
			
			try {
				log_date_start = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2013-06-28 00:00:00");
				log_date_end = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss").parse("2013-07-10 08:00:00");
			} catch (ParseException e) {
				// do nothing
				e.printStackTrace();
			}
			
		} catch (IOException e) {
			// do nothing
			e.printStackTrace();
		}
	}

	int addative_count = 0;

	public int find_Outlier(Bin m_Dt, int sentiment) {
		
		long lStartTime = System.currentTimeMillis();
		//some tasks
		
		 double m_log_EWMA = log_EWMA;
		 double m_log_EWMA_STD = log_EWMA_STD;
		 double m_log_PEWMA = log_PEWMA;
		 double m_log_PEWMA_STD = log_PEWMA_STD;
		 double m_log_STD_mean = log_STD_mean;
		 double m_log_STD = log_STD;
		 double m_log_MAD_median = log_MAD_median;
		 double m_log_MAD = log_MAD;
		
		
		boolean isLocalOutlier = false;
		boolean isWindowOutlier = false;
		Date testDate = null;
		try {
			testDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
					.parse("2013-07-02 15:45:00");
		} catch (ParseException e) {
			// DO nothing
			e.printStackTrace();
		}
		if (sentiment == 4 && (testDate.compareTo(m_Dt.getDate()) == 0)) {

			System.out.println("debug");
		}

		if (test_period_date == null) {
			test_period_date = AggregateUtilityFunctions.addMinutesToDate(
					TEST_PERIOD_MIN, m_Dt.getDate());
		}

		if (m_Dt.getDate().compareTo(test_period_date) >= 0) {

			double LDF = cal_dev_fact(m_Dt.getCount(), local_mean);
			//double test1 = (local_std_dev / local_mean);
			//SimpleDateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd");

			if (LDF > DEVIATION_THRESHOLD_LOCAL * (local_std_dev / local_mean)) {

				isLocalOutlier = true;
				m_Dt.setAnomaly_type(AnomalyType.LOCAL_ANOMALY);

				// double temp = (Math.abs(m_Dt.getCount() -
				// window_mean))/window_std_dev;
				double WDF = cal_dev_fact(m_Dt.getCount(), window_mean);

				if (WDF > DEVIATION_THRESHOLD_WINDOW
						* (window_std_dev / window_mean)) {
					addative_count++;
					// if(temp > DEVIATION_THRESHOLD_WINDOW){
					if (sentiment == 2) {

						// System.out.println("debug");
					}

					isWindowOutlier = true;
					m_Dt.setAnomaly_type(AnomalyType.WINDOW_ANOMALY);
					// generate ALERT
				} else {
					addative_count = 0;
				}

				window_list.add(m_Dt);
			}

			updateLocalContext(m_Dt);

			boolean isWindowSlided = slideWindowForward(m_Dt);
			if (isLocalOutlier || isWindowSlided) {
				updateWindowContext();
			}
		} else {
			test_list.add(m_Dt);

			local_mean = cal_mean(test_list);
			local_std_dev = cal_init_mean_dev(test_list);
			
			if(LOGGING){
			log_EWMA = local_mean;
			log_PEWMA = local_mean;
			
			log_EWMA_STD = local_std_dev;
			log_PEWMA_STD = local_std_dev;
			}
		}

		int finalReturnOutlier = 0;
		if (RETURN_OUTLIER_TYPE_WINDOW) {
			if (isWindowOutlier) {
				if (isWindowOutlier && addative_count < 2) {
					if (SHOW_INNOVATIVE_OUTLIERS) {
						finalReturnOutlier = 1;
					}
				} else if (isWindowOutlier && addative_count >= 2) {
					finalReturnOutlier = 2;

				}
			}
		} else {
			if (isLocalOutlier) {				
				finalReturnOutlier = 1;
			} else {				
				finalReturnOutlier = 0;
			}

		}

		if (LOGGING) {
			if(	m_Dt.getDate().compareTo(log_date_start) >= 0 && m_Dt.getDate().compareTo(log_date_end) <= 0 ) {
				try {
					testDate = null;
					try {
						testDate = new SimpleDateFormat("yyyy-MM-dd hh:mm:ss")
								.parse("2013-06-30 15:30:00");
					} catch (ParseException e) {
						// DO nothing
						e.printStackTrace();
					}
					if (current_sentiment == TweetSentiment.NEGATIVE && (testDate.compareTo(m_Dt.getDate()) == 0)) {

						System.out.println("debug");
					}

					
					String delimiter = "";
					
					String candidate_anomaly = "";
					String legitimate_anomaly = "";
					if(isLocalOutlier)
					{
						candidate_anomaly = String.valueOf(m_Dt.getCount());
						
					}
					if(finalReturnOutlier == 2)
					{
						legitimate_anomaly = String.valueOf(m_Dt.getCount());
					}
					
					delimiter = "\t";
					fw.write(dateFormat.format(
							m_Dt.getDate()) + delimiter
							+ m_Dt.getCount() + delimiter 
							+ candidate_anomaly + delimiter 
							+ legitimate_anomaly + delimiter 
							+ m_log_EWMA + delimiter 
							+ (m_log_EWMA + m_log_EWMA_STD) +delimiter 
							+ m_log_PEWMA + delimiter
							+ (m_log_PEWMA + m_log_PEWMA_STD) + delimiter 
							+ m_log_STD_mean  + delimiter 
							+ (m_log_STD_mean + m_log_STD) +delimiter 
							+ m_log_MAD_median +delimiter 
							+ (m_log_MAD_median +  m_log_MAD) 						
							+ System.getProperty("line.separator"));
					
					delimiter = ",";
					String str = dateFormat.format(
							m_Dt.getDate());
					
					
					fw_csv.write(dateFormat.format(
							m_Dt.getDate()) + delimiter
							+ m_Dt.getCount() + delimiter 
							+ candidate_anomaly + delimiter 
							+ legitimate_anomaly + delimiter 
							+ m_log_EWMA + delimiter 
							+ (m_log_EWMA + m_log_EWMA_STD) +delimiter
							+ (m_log_EWMA - m_log_EWMA_STD) +delimiter
							+ m_log_PEWMA + delimiter
							+ (m_log_PEWMA + m_log_PEWMA_STD) + delimiter
							+ (m_log_PEWMA - m_log_PEWMA_STD) + delimiter
							+ m_log_STD_mean  + delimiter 
							+ (m_log_STD_mean + m_log_STD) +delimiter 
							+ m_log_MAD_median +delimiter 
							+ (m_log_MAD_median +  m_log_MAD) 						
							+ System.getProperty("line.separator"));
					
					} catch (IOException e) {
						// Do Nothing
						e.printStackTrace();
					}
	
			}
		}

	

		// ********************************************************************
		long lEndTime = System.currentTimeMillis();
		 
		long difference = lEndTime - lStartTime;
	 
		System.out.println("Elapsed milliseconds: " + difference);
		return finalReturnOutlier;

	}

	void updateLocalContext(Bin m_Dt) {

		if (USE_PEWMA) {
			updateLocalContext_PEWMA(m_Dt);

		} else {
			updateLocalContext_EWMA(m_Dt);
			// updateLocalContext_PEWMA(m_Dt);
		}

		if (LOGGING) {
			log_updateLocalContext_EWMA(m_Dt);
			log_updateLocalContext_PEWMA(m_Dt);
		}
	}

	void updateWindowContext() {

		if (WINDOW_MAD_APPROACH) {
			calc_window_median_and_MAD();
		} else
			calc_window_mean_and_std_dev();

		if (LOGGING) {
			log_calc_window_median_and_MAD();
			log_calc_window_mean_and_std_dev();
		}
	}

	/**
	 * STEP 1: Probabilistic Exponential Moving Average
	 */
	void updateLocalContext_PEWMA(Bin m_Dt) {
		// 1. Probability function Guassian Distribution
		double z_score = cal_z_score(m_Dt.getCount(), local_mean, local_std_dev);

		// add z-score
		double var1 = -((z_score * z_score) / 2);
		double var2 = Math.exp(var1);
		// 2.
		double probabiltiy = const_a * var2;
		// 3.
		double alfa_t = (1 - const_beta * probabiltiy) * const_alpha_PEWMA;

		s1 = alfa_t * s1 + (1 - alfa_t) * m_Dt.getCount();

		s2 = alfa_t * s2 + (1 - alfa_t) * (m_Dt.getCount() * m_Dt.getCount());

		local_mean = s1;
		local_std_dev = Math.sqrt(Math.abs(s2 - (s1 * s1)));
	}

	void log_updateLocalContext_PEWMA(Bin m_Dt) {
		// 1. Probability function Guassian Distribution
		double z_score = cal_z_score(m_Dt.getCount(), log_PEWMA, log_PEWMA_STD);

		// add z-score
		double var1 = -((z_score * z_score) / 2);
		double var2 = Math.exp(var1);
		// 2.
		double probabiltiy = const_a * var2;
		// 3.
		double alfa_t = (1 - const_beta * probabiltiy) * const_alpha_PEWMA;

		log_s1 = alfa_t * log_s1 + (1 - alfa_t) * m_Dt.getCount();

		log_s2 = alfa_t * log_s2 + (1 - alfa_t) * (m_Dt.getCount() * m_Dt.getCount());

		log_PEWMA = log_s1;
		log_PEWMA_STD = Math.sqrt(Math.abs(log_s2 - (log_s1 * log_s1)));
	}

	/**
	 * STEP 1: Exponential Moving Average
	 */
	void updateLocalContext_EWMA(Bin m_Dt) {

		double diff = Math.abs(local_mean - m_Dt.getCount());

		local_mean = (1 - const_alpha_EWMA) * m_Dt.getCount()
				+ const_alpha_EWMA * local_mean;

		local_std_dev = (1 - const_alpha_EWMA) * diff + const_alpha_EWMA
				* local_std_dev;
	}

	void log_updateLocalContext_EWMA(Bin m_Dt) {

		double diff = Math.abs(log_EWMA - m_Dt.getCount());

		log_EWMA = (1 - const_alpha_EWMA) * m_Dt.getCount() + const_alpha_EWMA
				* log_EWMA;

		log_EWMA_STD = (1 - const_alpha_EWMA) * diff + const_alpha_EWMA
				* log_EWMA_STD;
	}

	/**
	 * STEP 2: Main function
	 */

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

	/**
	 * STEP 2: Standard Deviation Approach
	 */
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

	void log_calc_window_mean_and_std_dev() {
		double total_dev = 0.0f;
		log_STD_mean = cal_mean(window_list);
		for (Bin bin : window_list) {
			double distance = Math.abs(bin.getCount() - log_STD_mean)
					* Math.abs(bin.getCount() - log_STD_mean);
			total_dev += distance;
		}

		log_STD = Math.sqrt(total_dev / window_list.size());
	}

	/**
	 * STEP 2: MAD Approach
	 */
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

		double mad_median = getMedian(mad_list);

		window_std_dev = 1.4826 * mad_median; // MAD
	}

	void log_calc_window_median_and_MAD() {
		ArrayList<Double> median_list = new ArrayList<Double>();

		ArrayList<Double> mad_list = new ArrayList<Double>();
		for (Bin bin : window_list) {
			median_list.add((double) bin.getCount());
		}

		Collections.sort(median_list);
		log_MAD_median = getMedian(median_list); // Median

		for (Bin bin : window_list) {
			double e = Math.abs(bin.getCount() - log_MAD_median);
			mad_list.add(e);
		}

		Collections.sort(mad_list);

		double mad_median = getMedian(mad_list);

		log_MAD = 1.4826 * mad_median; // MAD
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
