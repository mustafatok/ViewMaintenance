package com.lin.client;

import com.lin.sql.JsqlParser;
import com.lin.sql.LogicalElement;
import com.lin.sql.SimpleLogicalPlan;
import com.lin.test.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;

import java.io.IOException;

/**
 * Created by munline on 2/15/16.
 */
public class ViewManager {

    private static void createViewTable(String tableName){
        // build an empty delta table with the following properties:
        //   =================================================
        //     table name: SQL(replace space with '_')_delta
        //   =================================================
        //                family:qualifier1_old
        //                family:qualifier1_new
        //                family:qualifier2_old
        //                family:qualifier2_new
        //                         .
        //                         .
        //                         .
        //                family:qualifiern_old
        //                family:qualifiern_new
        //
        // The actual qualifier will be determined in every coprocessor and being put
        // into the table in the coprocessor

        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;

        try {
            helper = HBaseHelper.getHelper(conf);
            helper.dropTable(tableName);
            helper.createTable(tableName, "colfam");

        } catch(IOException e) {
            e.printStackTrace();
        }
    }

    public static void putMetaData(String viewName, String type, String query, String queryType){
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;
        try {
            helper = HBaseHelper.getHelper(conf);
            helper.put("view_meta_data", viewName, "query", "string", query);
            helper.put("view_meta_data", viewName, "query", "type", queryType);
            helper.put("view_meta_data", viewName, "type", "value", type);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }
    public static void putTableView(String tableName, String viewName){
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;
        try {
            helper = HBaseHelper.getHelper(conf);
            helper.put("table_view", tableName, "views", viewName, "Test");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void createMaterializedView(String viewName, String query, String type){
        if(query == null)
            return;
        SimpleLogicalPlan simpleLogicalPlan = JsqlParser.parse(query, true);
        System.out.println(simpleLogicalPlan);

        createViewTable(viewName + "_delta");
        createViewTable(viewName);
        // TODO: Fix this to support joins.
        for(LogicalElement element = simpleLogicalPlan.getHead(); element != null; element = element.getNext() ) {
            element.setViewName(viewName);
            element.setMaterialize(true);
            if (element.getAggregationKey().equals("")) {
                putMetaData(viewName, type, query, "select");
            } else {
                putMetaData(viewName, type, query, "aggregation");
            }
            putTableView(element.getTableName(), viewName);
        }
        simpleLogicalPlan.getHead().execute();
    }

    public static void createMaterializedView(String viewName, String query) {
        createMaterializedView(viewName, query, "n"); // Not - Updatable
    }
    public static void createUpdatableView(String viewName, String query){
        createMaterializedView(viewName, query, "u"); // Updatable
    }

    public static void refreshView(String viewName){
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;
        String query = null;
        try {
            helper = HBaseHelper.getHelper(conf);
            query = helper.getValue("view_meta_data", viewName, "query", "string");
        } catch (IOException e) {
            e.printStackTrace();
        }
        createMaterializedView(viewName, query);
    }
}
