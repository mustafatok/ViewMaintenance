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

    private static void createViewTable(String tableName, String query){
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

        byte qType = JsqlParser.typeOfQuery(query);
        try {
            helper = HBaseHelper.getHelper(conf);
            helper.dropTable(tableName);
            helper.createTable(tableName, "colfam");

            if(qType == JsqlParser.AGGREGATION){
                helper.dropTable(tableName + "_delta");
                helper.createTable(tableName + "_delta", "colfam");
//                helper.createTable(tableName + "_delta", "colfam", "MIN", "MAX", "COUNT", "SUM", "AVG"); // TODO : Check if it is working without this..
            }else if(qType == JsqlParser.JOIN){
                helper.dropTable(tableName + "_delta");
                helper.createTable(tableName + "_delta", "colfam");
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }


    public static void putTableView(String tableName, String viewName, String query){
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;
        try {
            helper = HBaseHelper.getHelper(conf);
            helper.put("table_view", tableName, "views", viewName, query);
            helper.put("view_meta_data", viewName, "settings", "query", query);
            helper.put("view_meta_data", viewName, "tables", tableName, "Test"); //TODO: Change Test
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public static void createMaterializedView(String viewName, String query){
        if(query == null)
            return;
        SimpleLogicalPlan simpleLogicalPlan = JsqlParser.parse(query, true);
        System.out.println(simpleLogicalPlan);

        createViewTable(viewName, query);

        // TODO: Fix this to support joins.


        for(LogicalElement element = simpleLogicalPlan.getHead(); element != null; element = element.getNext() ) {
            element.setViewName(viewName);
            element.setMaterialize(true);
            putTableView(element.getTableName(), viewName, query);
        }
        simpleLogicalPlan.getHead().execute();
    }


    public static void refreshView(String viewName){
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;
        String query = null;
        try {
            helper = HBaseHelper.getHelper(conf);
            query = helper.getValue("view_meta_data", viewName, "settings", "query");
            createMaterializedView(viewName, query);

        } catch (IOException e) {
            e.printStackTrace();
        }
    }
}
