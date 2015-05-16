package job.dashboard.statistics;

import java.io.InputStreamReader;

public class BinarySearch {

	public static void main(String[] args)
	{
		// 1. desing a recursive method,
		//  INPUT: array, (int) start, end, target
		//  OUTPUT: return the ID in array
		
	//	BufferReader br = new BufferReader(new InputStreamReader(System.in));
		
		int [] array = {1,2,3,4,5,6,7,8,9};
		System.out.println(binary_search(array, 0, array.length, 9));
		
	}
	
	
	static int binary_search(int [] array, int start, int end, int target)
	{
		// 1. get the mid point
		// 2. 
		int mid = start + ((end -start)/2);
		
		if(array[mid] == target)
		{
			return mid;
		}else if (array[mid] < target)
		{
			return binary_search(array, mid + 1, end, target);
		}
		else
		{
			return binary_search(array, start, mid -1, target);
		}
		// 2. check if the mid point is the target
		// 3. check else if mid point is less then target, then second half
		// 4. else first half and call this function recur
		
	}
	
}
