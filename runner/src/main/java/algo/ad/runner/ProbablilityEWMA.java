package algo.ad.runner;

import java.util.ArrayList;

public class ProbablilityEWMA {
	static ArrayList<Double> z_scores_list = new ArrayList<Double>();
	
	static ArrayList<Double> z_scores_dev_list = new ArrayList<Double>();
	
	public static double get_z_scores_average()
	{
		double average = 0;
		double sum =0;
		if(z_scores_list.size() !=0){
		for (double e : z_scores_list) {
			sum += e;
		}
		average = sum/ z_scores_list.size();
		}
		else{
			average =0; 
		}
			
		return average;
	}
	
	public static double get_z_scores_dev_average()
	{
		double sum =0;
		for (double e : z_scores_dev_list) {
			sum += e;
		}
			
		return sum/z_scores_dev_list.size();
	}

	public static void main(String[] args) {
		// TODO Auto-generated method stub

		double input[] = {0.11,0.1,0.2,0.09,0.1,0.12,0.19,0.199,0.1,0.2,0.22,0.199,0.2,0.23,
				0.3,2.7,2.6,2.55,2,1,0.5,0.1,0.12,0.14,0.1,0.13,
				0.7,0.1,0.13,0.2,0.13,
				2,0.3,0.15,0.13,0.14,0.12,0.11,
				3.45,0.14,0.12,0.12,0.13,0.14,0.12,0.11,
				0.7,0.1,0.13,0.2,0.13};
		
		double const_a = 0.3989;
		double const_beta = 1;
		double const_alpha_PWMA = 0.99;
		double const_tou = 0.0044;
		double s1 = 1.66;
		double s2 = 0.51;
		double s2_test = 0.51;
		double X_tmean = 1.66;
		double X_deav = 0.51;
		boolean isAnomalous;
		int i = 1;
		for (double d : input) {
			isAnomalous = false;
			double Xt = d;
			
			// 1.
			double z_score = Xt - X_tmean / X_deav;
			
			// add z-score
			
			
			double var1 = -((z_score * z_score) / 2);
			double var2 = Math.exp(var1);
			// 2.
			double probabiltiy = const_a * var2;

			if(i ==19)
			{
			System.out.println("Ok wait");
			}
			// 3.
			double alfa_t = (1 - const_beta * probabiltiy) * const_alpha_PWMA;

			s1 = alfa_t * s1 + (1 - alfa_t) * Xt;

			s2 = alfa_t * s2 + (1 - alfa_t) * (Xt * Xt);
			
			double delta = Math.abs(Xt - X_tmean);
			double delta_sqr = delta * delta;
			s2_test = Math.sqrt(((1 - alfa_t)*(s2_test*s2_test) + alfa_t *(delta_sqr)));

			X_tmean = s1;
			X_deav = Math.sqrt(Math.abs(s2 - (s1 * s1)));
			
			// Global Outlier detection
			
			Double GDF_z = null;
			
			Double strd_dev_GDFs = null;
			
			if(probabiltiy < const_tou){
				isAnomalous = true;
				double mean_zscore = get_z_scores_average();
				GDF_z =  (mean_zscore - z_score)/mean_zscore;
				
				z_scores_list.add(z_score);
				
				//
				double diff = (Math.abs(z_score-mean_zscore));
				z_scores_dev_list.add(diff*diff);
				
				double mean_zscore_devs = get_z_scores_dev_average();
				
				strd_dev_GDFs= (1/mean_zscore) * (Math.sqrt(mean_zscore_devs));
				
			
			}
			
			System.out.println(i +":::"+isAnomalous+"::Xt:"+ Xt + "|s1:"+ s1 + "|s2:"+ s2 +
					"|X_deav: " + X_deav+
					"|X_deavTest: " + s2_test+
					"|z_score:"+ z_score + 
					"||prob:"+ probabiltiy +
					"|alfa_t:"+ alfa_t+
					"|GDF_z:" +GDF_z +
					"|strd_GDF:" +strd_dev_GDFs );
			//System.out.println(Xt + ","+ s1 + "," +probabiltiy);
			
			i++;
		}
	}

}
