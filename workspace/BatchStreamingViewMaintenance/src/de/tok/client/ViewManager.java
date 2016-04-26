package de.tok.client;

import de.tok.sql.JsqlParser;
import de.tok.sql.SelectElement;
import de.tok.sql.SimpleLogicalPlan;
import de.tok.utils.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;

import java.io.IOException;

/**
 * Created by tok on 2/15/16.
 */
public class ViewManager {

    private static void createViewTable(String viewName, String query){

        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;

        byte qType = JsqlParser.typeOfQuery(query);
        try {
            helper = HBaseHelper.getHelper(conf);
            helper.dropTable(viewName);
            helper.createTable(viewName, "colfam");
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
    private static void createDeltaViewTable(String viewName, String query){


        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;

        byte qType = JsqlParser.typeOfQuery(query);
        try {
            helper = HBaseHelper.getHelper(conf);
            if(qType == JsqlParser.AGGREGATION){
                helper.dropTable(viewName + "_delta");
                helper.createTable(viewName + "_delta", "colfam", "checkSum");
//                helper.createTable(tableName + "_delta", "colfam", "MIN", "MAX", "COUNT", "SUM", "AVG"); // TODO : Check if it is working without this..
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }
    private static void createDeltaViewTable(String viewName, String query, String leftTable, String rightTable){


        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;

        byte qType = JsqlParser.typeOfQuery(query);
        try {
            helper = HBaseHelper.getHelper(conf);
            if(qType == JsqlParser.JOIN){
                helper.dropTable(viewName + "_delta");
                helper.createTable(viewName + "_delta", leftTable, rightTable); // TODO : Delete joinFamily and try.
            }
        } catch(IOException e) {
            e.printStackTrace();
        }
    }


    public static void putTableView(String tableName, String viewName, String query){
       putTableView(tableName, viewName, query, "NA");
    }

    public static void putTableView(String tableName, String viewName, String query, String joinKey){
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;
        try {
            helper = HBaseHelper.getHelper(conf);
            helper.put("table_view", tableName, "views", viewName, query);
            helper.put("view_meta_data", viewName, "settings", "query", query);
            helper.put("view_meta_data", viewName, "tables", tableName, joinKey);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void putJoinInfo(String position, String tableName, String viewName){
        Configuration conf = HBaseConfiguration.create();
        HBaseHelper helper;
        try {
            helper = HBaseHelper.getHelper(conf);
            helper.put("view_meta_data", viewName, "tables", position, tableName);
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    public static void createMaterializedView(String viewName, String query, boolean returningResults){
        if(query == null)
            return;
        SimpleLogicalPlan simpleLogicalPlan = JsqlParser.parse(query, returningResults);
        System.out.println(simpleLogicalPlan);

        createViewTable(viewName, query);

        String leftTable = "";
        String rightTable = "";
        int i = 0;
        for(SelectElement element = (SelectElement)simpleLogicalPlan.getHead(); element != null; element = (SelectElement)element.getNext() ) {
            element.setViewName(viewName);
            element.setMaterialize(true);
            if(i <= 1) {
                putTableView(element.getTableName(), viewName, query, element.getJoinKey());
            }
            if(i == 0) {
                leftTable = element.getTableName();
                putJoinInfo("left", leftTable, viewName);
            }else if (i == 1) {
                rightTable = element.getTableName();
                putJoinInfo("right", rightTable, viewName);
            }else {
                element.setTableName(viewName + "_delta");
            }
            ++i;
        }

        if(simpleLogicalPlan.getSize() == 1){
            createDeltaViewTable(viewName, query);
        }else{
            createDeltaViewTable(viewName, query, leftTable, rightTable);
        }

        simpleLogicalPlan.getHead().execute();
    }

    public static void createMaterializedView(String viewName, String query){
        createMaterializedView(viewName, query, true);
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
