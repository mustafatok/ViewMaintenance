package com.lin.client;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.Random;

import org.apache.commons.cli.BasicParser;
import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.OptionBuilder;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.util.Bytes;

import com.lin.client.ViewManager;
import com.lin.sql.JsqlParser;
import com.lin.sql.SimpleLogicalPlan;
import com.lin.test.HBaseHelper;

public class CmdInterface {
	public static void main(String[] args) {
		// Read from input
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input;

		boolean esc = false;
		while (!esc) {
			try {
				System.out.print("Enter input: ");
				input = br.readLine();
				String[] argIn = input.split(" ");

				if (argIn[0].equals("quit")) {
					esc = true;
				} else if (argIn[0].equals("load")) {
					load(argIn);
				} else if (argIn[0].equals("test")){
					//test(argIn);
				} else if(argIn[0].equals("create")){
					if (argIn.length > 3 && argIn[1].equals("view")){
						createMaterializedView(argIn[2], input);
					} else if (argIn.length > 4 && argIn[1].equals("updatable") && argIn[2].equals("view")){
//						createUpdatableView(argIn[3], input);
					} else if (argIn[1].equals("table")){
						createTable(input);
						
					}
				} else {
					handleSQL(input, true);
				}
			}catch (IOException e) {
				e.printStackTrace();
			}catch (Exception generalEx) {
				generalEx.printStackTrace();
				System.out.println("Error in command");
			}
		}
	}
	
	private static void createMaterializedView(String viewName, String completeSQLStatement){
		String SQLStatement = completeSQLStatement.substring(("create view " + viewName).length() + 1);
		System.out.println("create view " + viewName + " " + SQLStatement);
		ViewManager.createMaterializedView(viewName, SQLStatement);

//		handleSQL(SQLStatement, true, viewName);
	}
	
//	private static void createUpdatableView(String viewName, String completeSQLStatement){
//		String SQLStatement = completeSQLStatement.substring(("create updatable view " + viewName).length() + 1);;
//
//		System.out.println("create updatable view " + viewName + " " + SQLStatement);
//		handleSQL(SQLStatement, true);
//	}
	
	private static void createTable(String SQLStatement){
		String rest = SQLStatement.substring("create table ".length());
		String[] args = rest.split(",");
		if(args.length == 0) return;
		String[] colfams = new String[args.length - 1];
		
		String tableName = args[0].replace("'", "").replace("\"", ""); 

		System.out.println(tableName);
		for (int i = 1; i < args.length; i++) {
			colfams[i - 1] = args[i].trim().replace("'", "").replace("\"", "");
			
			System.out.println(colfams[i - 1]);
		}
		
		
		
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper;
		try {
			helper = HBaseHelper.getHelper(conf);
			helper.dropTable(tableName);
			helper.createTable(tableName, colfams);
			
		} catch(IOException e){
			e.printStackTrace();
		}
	}
	

	/**
	 * Handle sql
	 * @param input
	 */
	public static void handleSQL(String input, boolean isReturningResults) {
		SimpleLogicalPlan simpleLogicalPlan = JsqlParser.parse(input, isReturningResults);
		System.out.println(simpleLogicalPlan);
		simpleLogicalPlan.getHead().execute();
	}

	/**
	 * Load command
	 * 
	 * @param args
	 */
	public static void load(String[] args) {
		// create Options object
		Options options = new Options();

		options.addOption(OptionBuilder.withLongOpt("name")
				.withDescription("the name of table").hasArg()
				.withArgName("NAME").create());
		
		options.addOption(OptionBuilder.withLongOpt("rows")
				.withDescription("the name of table").hasArg()
				.withArgName("ROWS").create());

		CommandLineParser parser = new BasicParser();
		try {
			CommandLine cmd = parser.parse(options, args);

			if (cmd.hasOption("name")) {
				String tableName = cmd.getOptionValue("name");

				System.out.println("loading " + tableName);
				
				// testtable 1
				if(tableName.equals("testtable1")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 1; i <= 100; i++){
							System.out.println("put row " + i);
							helper.put(tableName, "row"+i, "colfam", "qual1", 1, "val"+i);
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// testtable 2
				if(tableName.equals("testtable2")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 1; i <= 100; i++){
							System.out.println("put row " + i);
							helper.put(tableName, "row"+i, "colfam", "qual1", 1, "" + i);
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// testtable3
				if(tableName.equals("testtable3")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 1; i <= 100; i++){
							System.out.println("put row " + i);
							helper.put(tableName, "row"+i, "colfam", "joinkey", 1, "x" + i);
							helper.put(tableName, "row"+i, "colfam", "qualifierTesttable3", 1, "" + i);
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// testtable4
				if(tableName.equals("testtable4")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 100; i >= 0; i--){
							System.out.println("put row " + (101 - i));
							helper.put(tableName, "row"+(101 - i), "colfam", "joinkey", 1, "x" + i);
							helper.put(tableName, "row"+(101 - i), "colfam", "qualifierTesttable4", 1, "" + (101 - i));
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// testtable5
				if(tableName.equals("testtable5")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 100; i >= 0; i--){
							System.out.println("put row " + (101 - i));
							helper.put(tableName, "row"+(101 - i), "colfam", "aggKey", 1, "x" + i % 20);
							helper.put(tableName, "row"+(101 - i), "colfam", "value", 1, "" + (101 - i));
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// testtable6
				if(tableName.equals("testtable6")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 1; i >= 0; i--){
							System.out.println("put row " + (1 - i));
							helper.put(tableName, "K"+(1 - i), "colfam", "C1", 1, "x1");
							helper.put(tableName, "K"+(1 - i), "colfam", "C2", 1, "" + (1 - i));
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// testtable7
				if(tableName.equals("testtable7")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						String[] rows = {};
						List<String> stringArray = new ArrayList<String>();
						for(int i = 1; i >= 0; i--){
							System.out.println("put row " + (1 - i));
							helper.put(tableName, "L"+(1 - i), "colfam", "D1", 1, "x1");
							helper.put(tableName, "L"+(1 - i), "colfam", "D2", 1, "" + (1 - i));
						}
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// evaluateTable1
//				if(tableName.equals("evaluateTable1")){
//					Configuration conf = HBaseConfiguration.create();
//					HBaseHelper helper;
//					try {
//						helper = HBaseHelper.getHelper(conf);
//						helper.dropTable(tableName);
//						helper.createTable(tableName, "colfam");
//						
//						HTable table = new HTable(conf, tableName);
//						
//						for(int i = 0; i < 1000000; i++){
//							System.out.println("put row " + i);
//							Put put = new Put(("row" + i).getBytes());
//							put.add("colfam".getBytes(), 
//									"qualifier".getBytes(),
//									("v" + i).getBytes());
//							table.put(put);
//						}
//						table.close();
//					} catch(IOException e){
//						e.printStackTrace();
//					}
//				}
				
				// evaluateTable2
				if(tableName.equals("evaluateTable2")){
					Configuration conf = HBaseConfiguration.create();
					HBaseHelper helper;
					try {
						helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						helper.createTable(tableName, "colfam");
						
						HTable table = new HTable(conf, tableName);
						
						Random random = new Random();
						for(int i = 0; i < 10000; i++){
							System.out.println("put row " + i);
							Put put = new Put(("row" + i).getBytes());
							put.add("colfam".getBytes(), 
									"C1".getBytes(),
									("" + i).getBytes());
							System.out.println("put row " + i);
							table.put(put);
						}
						table.close();
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// evaluateTable3
				// 26 regions
				if(tableName.equals("evaluateTable3")){
					Configuration conf = HBaseConfiguration.create();
					try {
						HBaseHelper helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						byte[][] regioins = new byte[][]{
							Bytes.toBytes("A"),Bytes.toBytes("B"),Bytes.toBytes("C"),
							Bytes.toBytes("D"),Bytes.toBytes("E"),Bytes.toBytes("F"),
							Bytes.toBytes("G"),Bytes.toBytes("H"),Bytes.toBytes("I"),
							Bytes.toBytes("J"),Bytes.toBytes("K"),Bytes.toBytes("L"),
							Bytes.toBytes("M"),Bytes.toBytes("N"),Bytes.toBytes("O"),
							Bytes.toBytes("P"),Bytes.toBytes("Q"),Bytes.toBytes("R"),
							Bytes.toBytes("S"),Bytes.toBytes("T"),Bytes.toBytes("U"),
							Bytes.toBytes("V"),Bytes.toBytes("W"),Bytes.toBytes("X"),
							Bytes.toBytes("Y"),Bytes.toBytes("Z")
						};
						helper.createTable(tableName, regioins, "colfam");
						
						HTable table = new HTable(conf, tableName);
						
						Random random = new Random();
						for(int i = 0; i < 10000; i++){
							System.out.println("put row " + i);
							Put put = new Put((String.valueOf((char)((i % 26) + 65)) + i).getBytes());
							put.add("colfam".getBytes(), 
									"C1".getBytes(),
									("x" + (i % 20)).getBytes());
							put.add("colfam".getBytes(), 
									"C2".getBytes(),
									("" + i).getBytes());
							System.out.println("put row " + i);
							table.put(put);
						}
						table.close();
					} catch(IOException e){
						e.printStackTrace();
					}					
				}
				
				// evaluateTable4
				if(tableName.equals("evaluateTable4")){
					Configuration conf = HBaseConfiguration.create();
					try {
						HBaseHelper helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						byte[][] regioins = new byte[][]{
							Bytes.toBytes("A")
						};
						helper.createTable(tableName, regioins, "colfam");
						
						HTable table = new HTable(conf, tableName);
						
						Random random = new Random();
						for(int i = 0; i < 1000; i++){
							System.out.println("put row " + i);
							Put put = new Put((String.valueOf((char)((i % regioins.length) + 65)) + i).getBytes());
							put.add("colfam".getBytes(), 
									"C1".getBytes(),
									("x" + (i % 20)).getBytes());
							put.add("colfam".getBytes(), 
									"C2".getBytes(),
									("" + i).getBytes());
							System.out.println("put row " + i);
							table.put(put);
						}
						table.close();
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// evaluateTable5
				// 3 split
				if(tableName.equals("evaluateTable5")){
					Configuration conf = HBaseConfiguration.create();
					try {
						HBaseHelper helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						byte[][] regioins = new byte[][]{
							Bytes.toBytes("I"),Bytes.toBytes("R")
						};
						helper.createTable(tableName, regioins, "colfam");
						
						HTable table = new HTable(conf, tableName);
						
						Random random = new Random();
						rows = 3000;
						if (cmd.hasOption("rows")) {
							rows = Integer.parseInt(cmd.getOptionValue("rows")); 
						}
						for(int i = 0; i < rows; i++){
							System.out.println("put row " + i);
							Put put = new Put((String.valueOf((char)((i % 26) + 65)) + i).getBytes());
							put.add("colfam".getBytes(), 
									"C1".getBytes(),
									("x" + (i % 20)).getBytes());
							put.add("colfam".getBytes(), 
									"C2".getBytes(),
									("" + i).getBytes());
							System.out.println("put row " + i);
							table.put(put);
						}
						table.close();
					} catch(IOException e){
						e.printStackTrace();
					}
				}
				
				// evaluateTable6
				// 3 split
				if(tableName.equals("evaluateTable6")){
					Configuration conf = HBaseConfiguration.create();
					try {
						HBaseHelper helper = HBaseHelper.getHelper(conf);
						helper.dropTable(tableName);
						byte[][] regioins = new byte[][]{
							Bytes.toBytes("I"),Bytes.toBytes("R")
						};
						helper.createTable(tableName, regioins, "colfam");
						
						HTable table = new HTable(conf, tableName);
						
						Random random = new Random();
						for(int i = 0; i < 1000; i++){
							System.out.println("put row " + i);
							Put put = new Put((String.valueOf((char)((i % 26) + 65)) + i).getBytes());
							put.add("colfam".getBytes(), 
									"C1".getBytes(),
									("x" + (i % 20)).getBytes());
							put.add("colfam".getBytes(), 
									"C2".getBytes(),
									("" + i).getBytes());
							System.out.println("put row " + i);
							table.put(put);
						}
						table.close();
					} catch(IOException e){
						e.printStackTrace();
					}
				}
			} else {
				System.out.println("Must specify the table name");
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}
	
//	private static void test(String[] args) {
//		// create Options object
//		Options options = new Options();
//		
//		// test systematically
//		options.addOption(OptionBuilder.withLongOpt("sys")
//				.withDescription("test systematically").create());
//		
//		// test a single case
//		options.addOption(OptionBuilder.withLongOpt("case").hasArg()
//				.withArgName("CASE")
//				.withDescription("test systematically").create());
//		
//		// if returning the results or not
//		options.addOption(OptionBuilder.withLongOpt("returningResults")
//				.withDescription("return the results").create());
//		
//		// Evaluation will generate plot automatically after finish
//		options.addOption(OptionBuilder.withLongOpt("evaluate")
//				.withDescription("evaluate will generate plot automatically").create());
//		
//		// multiple threads
//		options.addOption(OptionBuilder.withLongOpt("client").hasArg()
//				.withArgName("CLIENT")
//				.withDescription("define the clients that connects at the same time").create());
//		
//		String[] testCase={
//				/*
//				 * Selection test cases
//				 */
//				// 0 test simple selection
//				"select colfam.qual1 from testtable1", 
//				// 1 test simple selection with condition
//				"select colfam.qual1 from testtable2 where colfam.qual1>20",
//				
//				/*
//				 * Aggregation test cases
//				 */
//				// 2 test aggregation without aggregation key
//				"select max(colfam.qual1), min(colfam.qual1), sum(colfam.qual1), avg(colfam.qual1), count(colfam.qual1) from testtable2",
//				// 3 test aggregation with aggregation key
//				"select max(colfam.value), min(colfam.value), sum(colfam.value), avg(colfam.value), count(colfam.value) from testtable5 group by colfam.aggKey",
//				
//				/*
//				 * Join test casestes
//				 */
//				// 4 test  simple join
//				"select testtable3.colfam.qualifierTesttable3, testtable4.colfam.qualifierTesttable4 from testtable3 join testtable4 on colfam.joinkey=colfam.joinkey",
//				// 5 Join with conditions
//				"select testtable3.colfam.qualifierTesttable3, testtable4.colfam.qualifierTesttable4 from testtable3 join testtable4 on colfam.joinkey=colfam.joinkey where testtable3.colfam.qualifierTesttable3>10",
//				// 6 full join with two row on one join key
//				"select testtable6.colfam.C2, testtable7.colfam.D2 from testtable6 join testtable7 on colfam.C1=colfam.D1",
//		};
//		
//		String[] evaluateCases = {
//				// Evaluate "Select"
//				// 1 million table auto splitting
//				// select full table without materialize
//				// select full table with materialize
//				"select colfam.C1 from evaluateTable2",
//				
//				/*
//				 *  Experiment 1
//				 *  1000 rows splitted into three region
//				 */
//				"select colfam.C1, colfam.C2 from evaluateTable5"
//		};
//
//		CommandLineParser parser = new BasicParser();
//		try {
//			CommandLine cmd = parser.parse(options, args);
//			
//			if(cmd.hasOption("sys")){
//				if(cmd.hasOption("case")){
//					String caseName = cmd.getOptionValue("case");
//					System.out.println("Testing case " + caseName);
//					if(cmd.hasOption("returningResults")){
//						if(cmd.hasOption("isMaterialize")){
//							handleSQL(testCase[Integer.parseInt(caseName)], true, true);
//						}else{
//							handleSQL(testCase[Integer.parseInt(caseName)], true, "");
//						}
//					}else{
//						if(cmd.hasOption("isMaterialize")){
//							handleSQL(testCase[Integer.parseInt(caseName)], true, true);
//						}else{
//							handleSQL(testCase[Integer.parseInt(caseName)], true, "");
//						}
//					}
//				}else{
//					for(int i = 0; i < testCase.length; i++){
//						System.out.println(testCase[i]);
//						if(cmd.hasOption("returningResults")){
//							handleSQL(testCase[i], true, true);
//						}else{
//							handleSQL(testCase[i], false, true);
//						}
//					}
//				}
//			}
//			
//			// Output plot data while evaluating
//			if(cmd.hasOption("evaluate")){
//				if(cmd.hasOption("case")){
//					String caseName = cmd.getOptionValue("case");
//					System.out.println("Evaluate case " + caseName);
//					int caseNo = Integer.parseInt(caseName);
//					
//					switch(caseNo){
//					// select full table with materialize and without materialize
//					case 0:
//						try {
//							// Record execution time without materialize
//							long startTime1 = System.nanoTime();
//							handleSQL(evaluateCases[caseNo], false, "");
//							long endTime1 = System.nanoTime();
//							long duration1 = (endTime1 - startTime1);  //divide by 1000000 to get milliseconds
//							
//							// Record execution time with materialize
//							long startTime2 = System.nanoTime();
//							handleSQL(evaluateCases[caseNo], false, true);
//							long endTime2 = System.nanoTime();
//							long duration2 = (endTime2 - startTime2);  //divide by 1000000 to get milliseconds
//							// save time in file and create a gnuplot script
//							String fileName = new Date().toString() + ".dat";
//							
//							PrintWriter writer;
//							writer = new PrintWriter(fileName, "UTF-8");
//							writer.println("# Evaluate full scan of evaluate table 1");
//							writer.println("# with and without materialize");
//							writer.println("materialize, " + (duration1 / 1000000));
//							writer.println("non-materialize, " + (duration2 / 1000000));
//							writer.close();
//							
//							PrintWriter writerForScript;
//							writerForScript = new PrintWriter("plotScript.gp", "UTF-8");
//							writerForScript.println("# Evaluate full scan of evaluate table 1");
//							writerForScript.println("# with and without materialize");
//							writerForScript.println("p \"" + fileName + "\" w l");
//							writerForScript.println("pause -1");
//							writerForScript.close();
//						} catch (FileNotFoundException
//								| UnsupportedEncodingException e) {
//							e.printStackTrace();
//						}
//						
//						// plot the data
//						try {
//							Runtime.getRuntime().exec("gnuplot plotScript.gp");
//						} catch (IOException e) {
//							e.printStackTrace();
//						}
//						
//						break;
//					case 1:
//						try {
//							// save time in file
//							String fileName = (new Date()).toString().replace(" ", "-")+"Experiment1.dat";
//							PrintWriter writer;
//							writer = new PrintWriter(fileName, "UTF-8");
//							writer.println("====================================");
//							writer.println(new Date().toString());
//							
//							if(cmd.hasOption("client")){
//								int clients = Integer.parseInt(cmd.getOptionValue("client"));
//							    
//							    for(int n = 1; n <= 10; n++){
//							    	// load table with 1000 * n rows
//							    	rows = 1000 * n;
//							    	// load table with specific rows
//									writer.println("# load evaluateTable 5 with " + rows + " rows");
//									CmdInterface.load(("load -name evaluateTable5 -rows " + rows).split(" "));
//							    	
//							    	final long[][] value = new long[clients][2];
//							    	
//							    	// start m clients
//							    	final CountDownLatch latch = new CountDownLatch(clients);
//							    	for(int m = 0; m < clients; m++){
//								    	
//									    Thread uiThread = new Experiment1(evaluateCases,caseNo,writer, rows, m){
//									        @Override
//									        public void run(){
//									        	// Record execution time without materialize
//												long startTime1 = System.nanoTime();
//												CmdInterface.handleSQL(evaluateCases[caseNo], false, "");
//												long endTime1 = System.nanoTime();
//												long duration1 = (endTime1 - startTime1);  //divide by 1000000 to get milliseconds
//												
////												// Record execution time with materialize
////												long startTime2 = System.nanoTime();
////												CmdInterface.handleSQL(evaluateCases[caseNo], false, true);
////												long endTime2 = System.nanoTime();
////												long duration2 = (endTime2 - startTime2);  //divide by 1000000 to get milliseconds
//									            
//												value[m][0] = duration1 / 1000000;
////												value[m][1] = duration2 / 1000000;
//												latch.countDown(); // Release await() in the test thread.
//									        }
//									    };
//									    uiThread.start();
//							    	}
//							    	
//							    	// Wait for countDown() in the UI thread. Or could uiThread.join();
//							    	try {
//										latch.await();
//									} catch (InterruptedException e) {
//										e.printStackTrace();
//									} 
//								    // value[][] holds results at this point.
//								    writer.println("# Evaluate full scan of evaluatetable5 with " + rows + " rows with query:");
//									writer.println("# " + evaluateCases[caseNo]);
//									writer.println("# with and without materialize");
//									for(int t = 0; t < value.length; t++){
//										writer.println(value[t][0]);
//									}
//									
//							    }
//							    
//								
//							}
//							
//							writer.close();
//							
//						} catch (FileNotFoundException
//								| UnsupportedEncodingException e) {
//							e.printStackTrace();
//						}
//						break;
//					default:
//						break;
//					}
//					
//				}else{
//					
//				}
//			}
//		} catch (ParseException e) {
//			e.printStackTrace();
//		}
//	}

	static int rows;
}
