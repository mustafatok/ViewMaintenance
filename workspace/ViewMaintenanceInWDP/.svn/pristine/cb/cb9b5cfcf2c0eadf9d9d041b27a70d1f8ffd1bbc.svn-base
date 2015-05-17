package de.webdataplatform.test;

import java.util.Random;

public class Zipfian {

	/**
	 * @param args
	 */
	public static void main(String[] args) {
		
		for (int i = 0; i < 100; i++) {
			
			System.out.println(getYourPositiveFunctionRandomNumber(0, 100));
		}

		
//		int N = 100;
//		
//		for (int n = 0; n < N; n++) {
//			
//			Double d = 1/(n*Math.log(1.78*N));
//			
//			int x = d.intValue();
//			
//			System.out.println(d);
//			
//		}

	
	}
	
	public static double linear(int i){
		
		return 100 - i;
		
	}
	
	public static double zipfian(int i){
		
		Double d = 1/(i*Math.log(1.78*100));
		return d;
		
	}
	
	
	public static int getYourPositiveFunctionRandomNumber(int startIndex, int stopIndex) {
	    //Generate a random number whose value ranges from 0.0 to the sum of the values of yourFunction for all the possible integer return values from startIndex to stopIndex.
	    double randomMultiplier = 0;
	    for (int i = startIndex; i <= stopIndex; i++) {
	        randomMultiplier += zipfian(i);//yourFunction(startIndex) + yourFunction(startIndex + 1) + .. yourFunction(stopIndex -1) + yourFunction(stopIndex)
	    }
	    Random r = new Random();
	    double randomDouble = r.nextDouble() * randomMultiplier;

	    //For each possible integer return value, subtract yourFunction value for that possible return value till you get below 0.  Once you get below 0, return the current value.  
	    int yourFunctionRandomNumber = startIndex;
	    randomDouble = randomDouble - zipfian(yourFunctionRandomNumber);
	    while (randomDouble >= 0) {
	        yourFunctionRandomNumber++;
	        randomDouble = randomDouble - zipfian(yourFunctionRandomNumber);
	    }

	    return yourFunctionRandomNumber;
	}
	
	
	

}
