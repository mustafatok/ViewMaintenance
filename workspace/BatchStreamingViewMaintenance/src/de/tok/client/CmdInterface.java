package de.tok.client;

import java.awt.*;
import java.io.*;
import java.util.*;
import java.util.List;
import java.util.concurrent.CountDownLatch;

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

import de.tok.sql.JsqlParser;
import de.tok.sql.SimpleLogicalPlan;
import de.tok.utils.HBaseHelper;
import org.apache.log4j.Level;
import org.apache.log4j.Logger;

public class CmdInterface {
	public static void main(String[] args) {
		// Read from input
		BufferedReader br = new BufferedReader(new InputStreamReader(System.in));
		String input;
//		Logger.getLogger("zookeeper").setLevel(Level.FATAL);
		Logger.getRootLogger().setLevel(Level.FATAL);
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
//					test(argIn);
					int type = Integer.parseInt(argIn[1]);
					int row = Integer.parseInt(argIn[2]);
					int sampling = Integer.parseInt(argIn[3]);

					if(type == select)
						incrementalTest(select, row, sampling, 1);
					else if(type == agg)
						for (int i = 1; i <= 10; ++i){
							System.out.println("Agg - " + i + ": ");
							incrementalTest(agg, row, sampling, row/i);
						}
					else if(type == join)
						for (int i = 1; i <= 10; ++i){
							System.out.println("Join - " + i + ": ");
							incrementalTest(join, row, sampling, row/i);
						}

//					incrementalTest(join, row);
				} else if(argIn[0].equals("create")){
					if (argIn.length > 3 && argIn[1].equals("view")){
						createMaterializedView(argIn[2], input, true);
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
	
	private static void createMaterializedView(String viewName, String completeSQLStatement, boolean returningResults){
		String SQLStatement = completeSQLStatement.substring(("create view " + viewName).length() + 1);
		System.out.println("create view " + viewName + " " + SQLStatement);
		ViewManager.createMaterializedView(viewName, SQLStatement, returningResults);

//		handleSQL(SQLStatement, true, viewName);
	}

	
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
//		System.out.println(simpleLogicalPlan);
		simpleLogicalPlan.getHead().execute();
	}

	/**
	 * Handle sql
	 * @param input
	 */
	public static void testSQL(String input, boolean isReturningResults) {
		String[] params = input.split(" ");

		if(params[0].equals("create")){
			if (params.length > 3 && params[1].equals("view")){
				createMaterializedView(params[2], input, true);
			}
		}
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
	
	private static void test(String[] args) {
		// create Options object
		Options options = new Options();

		// test systematically
		options.addOption(OptionBuilder.withLongOpt("sys")
				.withDescription("test systematically").create());

		// test a single case
		options.addOption(OptionBuilder.withLongOpt("case").hasArg()
				.withArgName("CASE")
				.withDescription("test systematically").create());

		// if returning the results or not
		options.addOption(OptionBuilder.withLongOpt("returningResults")
				.withDescription("return the results").create());

		// Evaluation will generate plot automatically after finish
		options.addOption(OptionBuilder.withLongOpt("evaluate")
				.withDescription("evaluate will generate plot automatically").create());

		// multiple threads
		options.addOption(OptionBuilder.withLongOpt("client").hasArg()
				.withArgName("CLIENT")
				.withDescription("define the clients that connects at the same time").create());

		String[] testCase={
				/*
				 * Selection test cases
				 */
				// 0 test simple selection
				"create view v0 select colfam.qual1 from testtable1",
				// 1 test simple selection with condition
				"create view v1 select colfam.qual1 from testtable2 where colfam.qual1>20",

				/*
				 * Aggregation test cases
				 */
				// 2 test aggregation without aggregation key
				"create view v2 select max(colfam.qual1), min(colfam.qual1), sum(colfam.qual1), avg(colfam.qual1), count(colfam.qual1) from testtable2",
				// 3 test aggregation with aggregation key
				"create view v3 select max(colfam.value), min(colfam.value), sum(colfam.value), avg(colfam.value), count(colfam.value) from testtable5 group by colfam.aggKey",

				/*
				 * Join test casestes
				 */
				// 4 test  simple join
				"create view v4 select testtable3.colfam.qualifierTesttable3, testtable4.colfam.qualifierTesttable4 from testtable3 join testtable4 on colfam.joinkey=colfam.joinkey",
				// 5 Join with conditions
				"create view v5 select testtable3.colfam.qualifierTesttable3, testtable4.colfam.qualifierTesttable4 from testtable3 join testtable4 on colfam.joinkey=colfam.joinkey where testtable3.colfam.qualifierTesttable3>10",
				// 6 full join with two row on one join key
				"create view v6 select testtable6.colfam.C2, testtable7.colfam.D2 from testtable6 join testtable7 on colfam.C1=colfam.D1",
		};

		String[] evaluateCases = {
				// Evaluate "Select"
				// 1 million table auto splitting
				// select full table without materialize
				// select full table with materialize
				"select colfam.C1 from evaluateTable2",

				/*
				 *  Experiment 1
				 *  1000 rows splitted into three region
				 */
				"select colfam.C1, colfam.C2 from evaluateTable5"
		};

		CommandLineParser parser = new BasicParser();
		try {
			CommandLine cmd = parser.parse(options, args);

			if(cmd.hasOption("sys")){
				if(cmd.hasOption("case")){
					String caseName = cmd.getOptionValue("case");
					System.out.println("Testing case " + caseName);

					testSQL(testCase[Integer.parseInt(caseName)], true);
				}else{
					for(int i = 0; i < testCase.length; i++){
						System.out.println(testCase[i]);
						testSQL(testCase[i], true);

					}
				}
			}

			// Output plot data while evaluating
			if(cmd.hasOption("evaluate")){
				if(cmd.hasOption("case")){
					String caseName = cmd.getOptionValue("case");
					System.out.println("Evaluate case " + caseName);
					int caseNo = Integer.parseInt(caseName);

					switch(caseNo){
					// select full table with materialize and without materialize
					case 0:
						try {

							// Record execution time with materialize
							long startTime2 = System.nanoTime();
							testSQL(evaluateCases[caseNo], false);
							long endTime2 = System.nanoTime();
							long duration2 = (endTime2 - startTime2);  //divide by 1000000 to get milliseconds
							// save time in file and create a gnuplot script
							String fileName = new Date().toString() + ".dat";

							PrintWriter writer;
							writer = new PrintWriter(fileName, "UTF-8");
							writer.println("# Evaluate full scan of evaluate table 1");
							writer.println("# with and without materialize");
							writer.println("non-materialize, " + (duration2 / 1000000));
							writer.close();

							PrintWriter writerForScript;
							writerForScript = new PrintWriter("plotScript.gp", "UTF-8");
							writerForScript.println("# Evaluate full scan of evaluate table 1");
							writerForScript.println("# with and without materialize");
							writerForScript.println("p \"" + fileName + "\" w l");
							writerForScript.println("pause -1");
							writerForScript.close();
						} catch (FileNotFoundException
								| UnsupportedEncodingException e) {
							e.printStackTrace();
						}

						// plot the data
						try {
							Runtime.getRuntime().exec("gnuplot plotScript.gp");
						} catch (IOException e) {
							e.printStackTrace();
						}

						break;
					case 1:
						try {
							// save time in file
							String fileName = (new Date()).toString().replace(" ", "-")+"Experiment1.dat";
							PrintWriter writer;
							writer = new PrintWriter(fileName, "UTF-8");
							writer.println("====================================");
							writer.println(new Date().toString());

							if(cmd.hasOption("client")){
								int clients = Integer.parseInt(cmd.getOptionValue("client"));

							    for(int n = 1; n <= 10; n++){
							    	// load table with 1000 * n rows
							    	rows = 1000 * n;
							    	// load table with specific rows
									writer.println("# load evaluateTable 5 with " + rows + " rows");
									CmdInterface.load(("load -name evaluateTable5 -rows " + rows).split(" "));

							    	final long[][] value = new long[clients][2];

							    	// start m clients
							    	final CountDownLatch latch = new CountDownLatch(clients);
							    	for(int m = 0; m < clients; m++){

									    Thread uiThread = new Experiment1(evaluateCases,caseNo,writer, rows, m){
									        @Override
									        public void run(){
									        	// Record execution time without materialize
												long startTime1 = System.nanoTime();
												CmdInterface.testSQL(evaluateCases[caseNo], false);
												long endTime1 = System.nanoTime();
												long duration1 = (endTime1 - startTime1);  //divide by 1000000 to get milliseconds

												value[m][0] = duration1 / 1000000;
//												value[m][1] = duration2 / 1000000;
												latch.countDown(); // Release await() in the test thread.
									        }
									    };
									    uiThread.start();
							    	}

							    	// Wait for countDown() in the UI thread. Or could uiThread.join();
							    	try {
										latch.await();
									} catch (InterruptedException e) {
										e.printStackTrace();
									}
								    // value[][] holds results at this point.
								    writer.println("# Evaluate full scan of evaluatetable5 with " + rows + " rows with query:");
									writer.println("# " + evaluateCases[caseNo]);
									writer.println("# with and without materialize");
									for(int t = 0; t < value.length; t++){
										writer.println(value[t][0]);
									}

							    }


							}

							writer.close();

						} catch (FileNotFoundException
								| UnsupportedEncodingException e) {
							e.printStackTrace();
						}
						break;
					default:
						break;
					}

				}else{

				}
			}
		} catch (ParseException e) {
			e.printStackTrace();
		}
	}

	private static void testInit(int type) {
		// testtable 1
		Configuration conf = HBaseConfiguration.create();
		HBaseHelper helper;

		try {
			helper = HBaseHelper.getHelper(conf);
			helper.dropTable("select_view");
			helper.dropTable("table_view");
			helper.dropTable("view_meta_data");
			helper.createTable("table_view", "views");
			helper.createTable("view_meta_data", "settings", "tables");

			if(type == select) {
				helper.dropTable("selecttable1");
				helper.createTable("selecttable1", "colfam");

				helper.dropTable("selecttable2");
				helper.createTable("selecttable2", "colfam");

				helper.dropTable("selecttable3");
				helper.createTable("selecttable3", "colfam");


				createMaterializedView("select_view_2", "create view select_view_2 select colfam.qual1 from selecttable2", false);


				createMaterializedView("select_view_3", "create view select_view_3 select colfam.qual1 from selecttable3", false);
				createMaterializedView("select_view_4", "create view select_view_4 select colfam.qual1 from selecttable3", false);
				createMaterializedView("select_view_5", "create view select_view_5 select colfam.qual1 from selecttable3", false);
				createMaterializedView("select_view_6", "create view select_view_6 select colfam.qual1 from selecttable3", false);
				createMaterializedView("select_view_7", "create view select_view_7 select colfam.qual1 from selecttable3", false);
				createMaterializedView("select_view_8", "create view select_view_8 select colfam.qual1 from selecttable3", false);
				createMaterializedView("select_view_9", "create view select_view_9 select colfam.qual1 from selecttable3", false);
				createMaterializedView("select_view_10", "create view select_view_10 select colfam.qual1 from selecttable3", false);
				createMaterializedView("select_view_11", "create view select_view_11 select colfam.qual1 from selecttable3", false);
				createMaterializedView("select_view_12", "create view select_view_12 select colfam.qual1 from selecttable3", false);
				createMaterializedView("select_view_13", "create view select_view_13 select colfam.qual1 from selecttable3", false);

			}else if(type == agg) {
				helper.dropTable("aggtable1");
				helper.createTable("aggtable1", "colfam");

				helper.dropTable("aggtable2");
				helper.createTable("aggtable2", "colfam");

				helper.dropTable("aggtable3");
				helper.createTable("aggtable3", "colfam");


				createMaterializedView("agg_view1", "create view agg_view1 select min(colfam.value) from aggtable1 group by colfam.aggKey", false);

				createMaterializedView("agg_view2", "create view agg_view2 select min(colfam.value) from aggtable2 group by colfam.aggKey", false);
				createMaterializedView("agg_view3", "create view agg_view3 select max(colfam.value) from aggtable2 group by colfam.aggKey", false);
				createMaterializedView("agg_view4", "create view agg_view4 select count(colfam.value) from aggtable2 group by colfam.aggKey", false);
				createMaterializedView("agg_view5", "create view agg_view5 select sum(colfam.value) from aggtable2 group by colfam.aggKey", false);
				createMaterializedView("agg_view6", "create view agg_view6 select avg(colfam.value) from aggtable2 group by colfam.aggKey", false);

				createMaterializedView("agg_view7", "create view agg_view7 select min(colfam.value), max(colfam.value), count(colfam.value), sum(colfam.value), avg(colfam.value) from aggtable3 group by colfam.aggKey", false);
				createMaterializedView("agg_view8", "create view agg_view8 select min(colfam.value), max(colfam.value), count(colfam.value), sum(colfam.value), avg(colfam.value) from aggtable3 group by colfam.aggKey", false);
				createMaterializedView("agg_view9", "create view agg_view9 select min(colfam.value), max(colfam.value), count(colfam.value), sum(colfam.value), avg(colfam.value) from aggtable3 group by colfam.aggKey", false);
				createMaterializedView("agg_view10", "create view agg_view10 select min(colfam.value), max(colfam.value), count(colfam.value), sum(colfam.value), avg(colfam.value) from aggtable3 group by colfam.aggKey", false);
				createMaterializedView("agg_view11", "create view agg_view11 select min(colfam.value), max(colfam.value), count(colfam.value), sum(colfam.value), avg(colfam.value) from aggtable3 group by colfam.aggKey", false);

			}else if(type == join){
				helper.dropTable("jointable11");
				helper.dropTable("jointable12");
				helper.createTable("jointable11", "colfam");
				helper.createTable("jointable12", "colfam");

				createMaterializedView("join_view", "create view join_view select jointable11.colfam.qualifierTesttable3, jointable12.colfam.qualifierTesttable4 from jointable11 join jointable12 on colfam.joinkey=colfam.joinkey", false);

			}
		}
		catch(IOException e){
			e.printStackTrace();
		}

	}


	private static void incrementalTest(int type, int row, int sampling, int aggDivisor){
			testInit(type);
//			LinkedList<Point>[] coordinates = new LinkedList[3];
			ArrayList<LinkedList<Point>> coordinates = new ArrayList<LinkedList<Point>>(3);

			for(int i = 0; i < 3; ++i) coordinates.add(i, new LinkedList<Point>());

			long totalTime[] = new long[3];
			long sum[] = new long[3];
			long cnt[] = new long[3];

			System.out.println("Started!");
			System.out.println("Insert : ");
			for(int i = 1; i <= (type == join?2*row:row); i++){

				String in[] = new String[3];

				if(type == select) {
					in[0] = "INSERT INTO selecttable1(row, colfam.qual1) VALUES(row" + i + ", val" + i + ")";
					in[1] = "INSERT INTO selecttable2(row, colfam.qual1) VALUES(row" + i + ", val" + i + ")";
					in[2] = "INSERT INTO selecttable3(row, colfam.qual1) VALUES(row" + i + ", val" + i + ")";
				}else if(type == agg) {
					in[0] = "INSERT INTO aggtable1(row, colfam.aggKey, colfam.value) VALUES(row" + i + ", a" + (i % aggDivisor) + ", " + i + ")";
					in[1] = "INSERT INTO aggtable2(row, colfam.aggKey, colfam.value) VALUES(row" + i + ", a" + (i % aggDivisor) + ", " + i + ")";
					in[2] = "INSERT INTO aggtable3(row, colfam.aggKey, colfam.value) VALUES(row" + i + ", a" + (i % aggDivisor) + ", " + i + ")";
				}else if(type == join){
					if(i <= row/2)
						in[0] = "INSERT INTO jointable11(row, colfam.joinKey, colfam.qualifierTesttable3) VALUES(row" + i + ", j" + (i % aggDivisor) + ", " + i + ")";
					else if(i <= row)
						in[0] = "INSERT INTO jointable12(row, colfam.joinKey, colfam.qualifierTesttable4) VALUES(row" + (i - row/2) + ", j" + ((i - row/2) % aggDivisor) + ", " + (row + 1 - (i - row/2)) + ")";
					else if(i <= 3*row/2)
						in[0] = "INSERT INTO jointable11(row, colfam.joinKey, colfam.qualifierTesttable3) VALUES(row" + (i - row/2) + ", j" + ((i - row/2) % aggDivisor) + ", " + (i - row/2) + ")";
					else
						in[0] = "INSERT INTO jointable12(row, colfam.joinKey, colfam.qualifierTesttable4) VALUES(row" + (i - row) + ", j" + (i % aggDivisor) + ", " + (2*row + 1 - i) + ")";
				}


				long duration[] = new long[3];

				for (int j = 0; j < 3; ++j){
					if(j > 0 && type == join)
						break;
					duration[j] = analyze(in[j]);
					totalTime[j] += duration[j];
					if(i % sampling == 0) {
						coordinates.get(j).add(new Point(i, (int) ((totalTime[j])/sampling) / 1000000));
						sum[j] = sum[j] + (totalTime[j] / sampling);
						cnt[j] = cnt[j] + 1;
						totalTime[j] = 0;
					}
				}
			}

			for (int j = 0; j < 3; ++j){
				if(j > 0 && type == join)
					break;
				System.out.println("Results - " + (j + 1) + " : " + convert(coordinates.get(j)));
				System.out.println("Avg - " + (j + 1) + " : " + (sum[j] / cnt[j]) / 1000000);
			}




			System.out.println("Update1 : ");

			coordinates = new ArrayList<LinkedList<Point>>(3);

			for(int i = 0; i < 3; ++i)
				coordinates.add(i, new LinkedList<Point>());

			totalTime = new long[3];
			sum = new long[3];
			cnt = new long[3];

			for(int i = 1; i <= row; i++){

				String in[] = new String[3];
				if(type == select) {
					in[0] = "UPDATE selecttable1 SET row = row" + i + ", colfam.qual1 = " + (row + 1 - i);
					in[1] = "UPDATE selecttable2 SET row = row" + i + ", colfam.qual1 = " + (row + 1 - i);
					in[2] = "UPDATE selecttable3 SET row = row" + i + ", colfam.qual1 = " + (row + 1 - i);
				}else if(type == agg) {
					in[0] = "UPDATE aggtable1 SET row = row" + i + ", colfam.value = " + (row + 1 - i);
					in[1] = "UPDATE aggtable2 SET row = row" + i + ", colfam.value = " + (row + 1 - i);
					in[2] = "UPDATE aggtable3 SET row = row" + i + ", colfam.value = " + (row + 1 - i);
				}else if(type == join) {
					in[0] = "UPDATE jointable11 SET row = row" + i + ", colfam.qualifierTesttable3 = " + (row + 1 - i);
				}
				long duration[] = new long[3];

				for (int j = 0; j < 3; ++j){
					if(j > 0 && type == join)
						break;
					duration[j] = analyze(in[j]);
					totalTime[j] += duration[j];
					if(i % sampling == 0) {
						coordinates.get(j).add(new Point(i, (int) ((totalTime[j])/sampling) / 1000000));
						sum[j] = sum[j] + (totalTime[j] / sampling);
						cnt[j] = cnt[j] + 1;
						totalTime[j] = 0;
					}
				}
			}
			for (int j = 0; j < 3; ++j){
				if(j > 0 && type == join)
					break;
				System.out.println("Results - " + (j + 1) + " : " + convert(coordinates.get(j)));
				System.out.println("Avg - " + (j + 1) + " : " + (sum[j] / cnt[j]) / 1000000);
			}

			if(type == agg || type == join) {


				System.out.println("Update2 : ");

				coordinates = new ArrayList<LinkedList<Point>>(3);

				for(int i = 0; i < 3; ++i)
					coordinates.add(i, new LinkedList<Point>());

				totalTime = new long[3];
				sum = new long[3];
				cnt = new long[3];


				for(int i = 1; i <= row; i++){

					String in[] = new String[3];
					if(type == agg) {
						in[0] = "UPDATE aggtable1 SET row = row" + i + ", colfam.aggKey = a" + (row + 1 - i) % aggDivisor;
						in[1] = "UPDATE aggtable2 SET row = row" + i + ", colfam.aggKey = a" + (row + 1 - i) % aggDivisor;
						in[2] = "UPDATE aggtable3 SET row = row" + i + ", colfam.aggKey = a" + (row + 1 - i) % aggDivisor;
					} else if(type == join) {
						in[0] = "UPDATE jointable11 SET row = row" + i + ", colfam.joinKey = j" + (row + 1 - i) % aggDivisor;
					}
					long duration[] = new long[3];

					for (int j = 0; j < 3; ++j){
						if(j > 0 && type == join)
							break;
						duration[j] = analyze(in[j]);
						totalTime[j] += duration[j];
						if(i % sampling == 0) {
							coordinates.get(j).add(new Point(i, (int) ((totalTime[j])/sampling) / 1000000));
							sum[j] = sum[j] + (totalTime[j] / sampling);
							cnt[j] = cnt[j] + 1;
							totalTime[j] = 0;
						}
					}
				}
				for (int j = 0; j < 3; ++j){
					if(j > 0 && type == join)
						break;
					System.out.println("Results - " + (j + 1) + " : " + convert(coordinates.get(j)));
					System.out.println("Avg - " + (j + 1) + " : " + (sum[j] / cnt[j]) / 1000000);
				}


			}





			System.out.println("Delete : ");

			coordinates = new ArrayList<LinkedList<Point>>(3);

			for(int i = 0; i < 3; ++i)
				coordinates.add(i, new LinkedList<Point>());


			totalTime = new long[3];
			sum = new long[3];
			cnt = new long[3];

			for(int i = 1; i <= row; i++){

				String in[] = new String[3];

				if(type == select) {
					in[0] = "DELETE FROM selecttable1 WHERE row = row" + i;
					in[1] = "DELETE FROM selecttable2 WHERE row = row" + i;
					in[2] = "DELETE FROM selecttable3 WHERE row = row" + i;
				}else if(type == agg) {
					in[0] = "DELETE FROM aggtable1 WHERE row = row" + i;
					in[1] = "DELETE FROM aggtable2 WHERE row = row" + i;
					in[2] = "DELETE FROM aggtable3 WHERE row = row" + i;
				}else if(type == join){
					in[0] = "DELETE FROM jointable11 WHERE row = row" + i;
				}

				long duration[] = new long[3];

				for (int j = 0; j < 3; ++j){
					if(j > 0 && type == join)
						break;
					duration[j] = analyze(in[j]);
					totalTime[j] += duration[j];
					if(i % sampling == 0) {
						coordinates.get(j).add(new Point(i, (int) ((totalTime[j])/sampling) / 1000000));
						sum[j] = sum[j] + (totalTime[j] / sampling);
						cnt[j] = cnt[j] + 1;
						totalTime[j] = 0;
					}
				}
			}
			for (int j = 0; j < 3; ++j){
				if(j > 0 && type == join)
					break;
				System.out.println("Results - " + (j + 1) + " : " + convert(coordinates.get(j)));
				System.out.println("Avg - " + (j + 1) + " : " + (sum[j] / cnt[j]) / 1000000);
			}

	}

	private static long analyze(String query){
		long startTime;
		long endTime;

		SimpleLogicalPlan simpleLogicalPlan = JsqlParser.parse(query, false);
		//	parserEnd = System.nanoTime();

		startTime = System.nanoTime();
		simpleLogicalPlan.getHead().execute();
		endTime = System.nanoTime();

		return (endTime - startTime);  //divide by 1000000 to get milliseconds

	}
	private static String convert(LinkedList<Point> coordinates){
		String s = "";
		for (Point p:coordinates) {
//			if(!s.equals(""))
//				s += ", ";

			s += "(" + p.getX() + ", " + p.getY() + ")";
		}
		return s;

	}
	static int rows;

	private static final int select = 1;
	private static final int agg = 2;
	private static final int join = 3;

}
