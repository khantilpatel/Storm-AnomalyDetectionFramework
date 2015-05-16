package algo.ad.utility;

import java.util.ArrayList;
import java.util.Date;
import java.util.List;

import common.feeder.utility.AggregateUtilityFunctions;

public class Algorithm_Peak_Windows {
	// Constants
	final int WINDOW_SIZE_MIN = 8640; // 2 days
	final int INIT_COUNT = 5; // initial bin counts
	final int TOU = 6;
	final float CONST_ALFA = 0.125f;

	List<Bin> init_bins = new ArrayList<Bin>();

	// Counts for control loop
	int init_bin_count = 0;

	// Flags for control loop
	boolean is_window_in_progress = false;
	boolean is_peak_fall = false;
	// Bin Store variables
	Bin C_start;
	Bin C_i; // Its Ci
	Bin C_i_1; // Its Ci-1
	public List<PickWindow> peak_Windows = new ArrayList<PickWindow>();
	protected List<Bin> current_bins = new ArrayList<Bin>();
	// Calculation variables
	Float current_mean = null;
	Float current_mean_dev;

	public void update_window(Bin current_bin) {
		if (!peak_Windows.isEmpty()) {
			Date window_start_date = AggregateUtilityFunctions
					.minusMinutesToDate(WINDOW_SIZE_MIN, current_bin.getDate());

			PickWindow peak_window = peak_Windows.get(0);
			while (peak_window.getEndTime().compareTo(window_start_date) < 0 && !peak_Windows.isEmpty()) {

				peak_Windows.remove(0);
				
				if(!peak_Windows.isEmpty())
				peak_window = peak_Windows.get(0);
			}
		}
		
	}

	public boolean detect_anomaly(PickWindow current_peak)
	{
		boolean isAnomalous =  true;
		
		for(PickWindow peak : peak_Windows) {
			if(current_peak.getMaxValue() < peak.getMaxValue())
			{
				isAnomalous = false;
				break;
			}
		}
		
		return isAnomalous;
		
	}
	
	public boolean find_peak_window(Bin bin, int currentSentement) {
		boolean isAnomalous = false; 
		C_i = bin;
		if (current_mean == null) {
			current_mean = (float) C_i.getCount();
		}

		if (count_for_init(C_i)) {

			if(5 ==C_i.getCount() && 0 == currentSentement)
			{
				float testValue = UtilityFunctions.cal_z_score(C_i.getCount(),
						current_mean, current_mean_dev);
				testValue=testValue +0;
			}
			// Check if current bin is big enough for a window.
			if (is_window_in_progress
					|| UtilityFunctions.cal_z_score(C_i.getCount(),
							current_mean, current_mean_dev) > TOU
					&& C_i.getCount() > C_i_1.getCount()) {

				// If New Window set Start Bin
				if (C_start == null) {
					C_start = C_i_1;
					is_window_in_progress = true;
					current_bins.clear(); // Clear the bin list for new peak
					current_bins.add(C_i_1);
				}

				// Go upto Peak
				if (!is_peak_fall && C_i.getCount() > C_i_1.getCount()) {
					current_bins.add(C_i);// Add to list of bins for current
											// window.
					current_mean_dev = UtilityFunctions.update_mean_dev(
							CONST_ALFA, current_mean, current_mean_dev,
							C_i.getCount());
					current_mean = UtilityFunctions.update_mean(CONST_ALFA,
							current_mean, C_i.getCount());

				}
				// Now Fall down from Peak
				else if (C_i.getCount() > C_start.getCount()) {
					if (UtilityFunctions.cal_z_score(C_i.getCount(),
							current_mean, current_mean_dev) > TOU
							&& C_i.getCount() > C_i_1.getCount()) {
						current_bins.add(C_i); // Add to list of bins for
												// current Peak.
						if(C_start == null)
						{						
							System.out.println("Got it");
						}
						
						if(C_i_1 == null)
						{						
							System.out.println("Got it");
						}
						PickWindow new_peak_window =  UtilityFunctions.add_to_Windows(peak_Windows,
								current_bins, C_start, C_i_1);
						
						isAnomalous = this.detect_anomaly(new_peak_window);
						
						current_mean_dev = UtilityFunctions.update_mean_dev(
								CONST_ALFA, current_mean, current_mean_dev,
								C_i.getCount());
						current_mean = UtilityFunctions.update_mean(CONST_ALFA,
								current_mean, C_i.getCount());
						is_peak_fall = false;
						//current_bins.clear();
						C_start = C_i;

					} else {
						is_peak_fall = true;
						current_bins.add(C_i); // Add to list of bins for
												// current Peak.
						current_mean_dev = UtilityFunctions.update_mean_dev(
								CONST_ALFA, current_mean, current_mean_dev,
								C_i.getCount());
						current_mean = UtilityFunctions.update_mean(CONST_ALFA,
								current_mean, C_i.getCount());

					}
				} else {
		
					if(C_start == null)
					{						
						System.out.println("Got it");
					}
					
					if(current_bins.size() == 0)
					{						
						System.out.println("Got it");
					}
					PickWindow new_peak_window = UtilityFunctions.add_to_Windows(peak_Windows, current_bins,
							C_start, C_i);
					
					isAnomalous = this.detect_anomaly(new_peak_window);
					
					current_mean_dev = UtilityFunctions.update_mean_dev(
							CONST_ALFA, current_mean, current_mean_dev,
							C_i.getCount());
					current_mean = UtilityFunctions.update_mean(CONST_ALFA,
							current_mean, C_i.getCount());
					is_peak_fall = false;
					is_window_in_progress = false;
					C_start = null;
			
											// Peak.
				}
			} else {
				current_mean_dev = UtilityFunctions.update_mean_dev(CONST_ALFA,
						current_mean, current_mean_dev, C_i.getCount());
				current_mean = UtilityFunctions.update_mean(CONST_ALFA,
						current_mean, C_i.getCount());

			}
		}
		C_i_1 = C_i;
		// else {
		// current_mean = UtilityFunctions.update_mean(CONST_ALFA,
		// current_mean, C_i.getCount());
		// current_mean_dev = UtilityFunctions.update_mean_dev(CONST_ALFA,
		// current_mean_dev, C_i.getCount());
		// }
		return isAnomalous;
	}

	boolean count_for_init(Bin bin) {
		boolean result = false;

		if (init_bins.size() < INIT_COUNT) {
			init_bins.add(bin);

			// if (init_bins.size() <= INIT_COUNT) {
			current_mean_dev = UtilityFunctions.cal_init_mean_dev(init_bins);
			current_mean = UtilityFunctions.cal_mean(init_bins);
			C_i_1 = bin;
			// }
		} else {
			result = true;
		}

		return result;
	}

}
