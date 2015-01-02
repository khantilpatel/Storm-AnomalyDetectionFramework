package algo.ad.utility;

import java.util.List;

public class UtilityFunctions {

	// static float calculate_mean(Bin bin, int index_start, int index_end)
	// {
	// for
	//
	// return index_end;
	//
	// }

	static float cal_mean(List<Bin> bins) {
		float total = 0;
		for (Bin bin : bins) {
			total += bin.getCount();
		}
		return total / bins.size();
	}

	static float cal_init_mean_dev(List<Bin> bins) {
		float total_dev = 0.0f;
		float mean = cal_mean(bins);
		for (Bin bin : bins) {
			float distance = Math.abs(bin.getCount() - mean);
			total_dev += distance;
		}
		
		float result = total_dev / bins.size();
		return result;
	}

	static float update_mean(float CONST_ALFA, float oldMean, float updateValue) {
		float result = CONST_ALFA * updateValue + (1 - CONST_ALFA) * oldMean;
		
		return result;
	}

	static float update_mean_dev(float CONST_ALFA, float oldMean, float oldMeanDev,
			float updateValue) {
		float diff = Math.abs(oldMean - updateValue);
		float result = CONST_ALFA * diff + (1 - CONST_ALFA) * oldMeanDev;
		
		return result;
	}
	
	
	static float cal_z_score(float currentValue, float currentMean, float currentMeanDev )
	{
		float result = 0 ;
		if( 0 != currentMeanDev){
		float absValue = Math.abs(currentValue - currentMean);
		 result =  absValue/currentMeanDev;
		}
		return result;
	}
	
	static PickWindow add_to_Windows(List<PickWindow> current_windows, List<Bin> current_bins, Bin bin_start, Bin bin_end)
	{
		boolean result = true;
		//1. Get the Max and Min range from current list of bins
		int max = current_bins.get(0).getCount();
		int min = current_bins.get(0).getCount() ;
		
		int i =0;
		for (Bin bin : current_bins) {
			i++;
			//System.out.println("Bin size");
			//System.out.println(i+"||Test add:-Size"+current_bins.size()+" ||Date" +bin.getDate() +"||count:"+bin.getCount()+"||Max:"+max );
			if(bin.getCount() > max)
				max = bin.getCount();
			
			if(bin.getCount() < min)
				min = bin.getCount();
		}
		
		// 2. calculate mean 
		float mean = cal_mean(current_bins);
		
		// 3. calculate bean count
		int bean_count = current_bins.size();
		
		PickWindow window = new PickWindow();
		
		window.setStartTime(bin_start.getDate());
		window.setEndTime(bin_end.getDate());
		
		window.setMaxValue(max);
		window.setMinValue(min);
		
		window.setBeanCount(bean_count);
		window.setMeanValue(mean);
		window.setMeanDev(0);
		
		// Check for anomaly
		
		
		current_windows.add(window);
		//window.g
		return window;
	}
}
