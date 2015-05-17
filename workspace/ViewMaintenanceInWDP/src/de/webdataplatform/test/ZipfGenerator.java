package de.webdataplatform.test;

import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

import org.apache.commons.math3.distribution.ZipfDistribution;


public class ZipfGenerator {
	 private Random rnd = new Random(System.currentTimeMillis());
	 private int size;
	 private double skew;
	 private double bottom = 0;
	 
	 public ZipfGenerator(int size, double skew) {
	  this.size = size;
	  this.skew = skew;
	 
	  for(int i=1;i < size; i++) {
	  this.bottom += (1/Math.pow(i, this.skew));
	  }
	 }
	 
	 // the next() method returns an random rank id.
	 // The frequency of returned rank ids are follows Zipf distribution.
	 public int next() {
	   int rank;
	   double friquency = 0;
	   double dice;
	 
	   rank = rnd.nextInt(size);
//	   System.out.println(rank);
	   friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
//	   System.out.println(friquency);
	   dice = rnd.nextDouble();
//	   System.out.println(dice);
	   
	   while((dice > friquency)) {
	     rank = rnd.nextInt(size);
//	     System.out.println(rank);
	     friquency = (1.0d / Math.pow(rank, this.skew)) / this.bottom;
//	     System.out.println(friquency);
	     dice = rnd.nextDouble();
//	     System.out.println(dice);
	   }
	 
	   return rank;
	 }
	 
	 // This method returns a probability that the given rank occurs.
	 public double getProbability(int rank) {
	   return (1.0d / Math.pow(rank, this.skew)) / this.bottom;
	 }
	 
	 public static void main(String[] args) {
//	   if(args.length != 2) {
//	     System.out.println("usage: ./zipf size skew");
//	     System.exit(-1);
//	   }
		
			
//			Random random = new Random();
//			ZipfDistribution zd = new ZipfDistribution(new Long(numOfKeys).intValue(), 1); 
//			
//			int sysoutCount=0;
//			long startingTime = new Date().getTime();
//
//			
//			for (long i = 0; i < numOfOperations; i++) {
//				
//				sysoutCount++;	
//
//				long k = tableDefinition.getPrimaryKey().getStartRange();
//				
//				if(distribution.equals("uniform"))k+=random.nextInt(new Long(numOfKeys).intValue());
//				if(distribution.equals("zipf"))k+=zd.sample();
		 
	 
//	   ZipfGenerator zipf = new ZipfGenerator(Integer.valueOf(10), Double.valueOf(1));
		 
	   int amount = 1000; 
	   int transactions = 1000000; 
	   
	   ZipfDistribution zd = new ZipfDistribution(amount, 1); 
//	   for(int i=1;i <= 10; i++)
//	     System.out.println(i+" "+zd.probability(i));
	   
//	   int[] x = zd.sample(100);
//	   
//	   System.out.println(x);
	
	   Random random = new Random();
	   
	   Map<Integer, Integer> map = new HashMap<Integer, Integer>();
	   
	   long startTime = new Date().getTime();
//	   for(int i=1;i <= transactions; i++){
//		   
//		   int x = zd.sample();
//			   
//	   }
//	   System.out.println("generated "+transactions+" transaction with zipf dist:"+(new Date().getTime() - startTime)/1000+" s");
//	   
	   startTime = new Date().getTime();
	   for(int i=1;i <= transactions; i++){
		   
		   int x = random.nextInt(amount); 
			   
	   }
	   System.out.println("generated "+transactions+" transaction with uniform dist:"+(new Date().getTime() - startTime)+" ms");
	   ZipfGenerator zipf = new ZipfGenerator(amount,1);
	   startTime = new Date().getTime();
	   int countOne = 0;
	   int countNine = 0;
	   for(int i=1;i <= transactions; i++){
		   
		   int x = zipf.next();
//		   System.out.println(x);
//		   System.out.println("-----");
//			if(x == 1)countOne++;  
//			if(x == 9)countNine++;  
	   }
	   System.out.println("generated "+transactions+" transaction with zipf dist:"+(new Date().getTime() - startTime)+" ms");
//	   System.out.println("countOne: "+countOne);	  
//	   System.out.println("countNine:"+countNine);	
//	   System.out.println(map);
	   
	 }
	}

