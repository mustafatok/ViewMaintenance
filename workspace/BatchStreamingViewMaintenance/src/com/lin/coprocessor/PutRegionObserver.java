package com.lin.coprocessor;
import com.lin.client.ViewManager;
import com.lin.test.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class PutRegionObserver extends BaseRegionObserver{

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put, WALEdit edit, Durability durability) throws IOException {
        TableName table = observerContext.getEnvironment().getRegionInfo().getTable();
        if(!table.isSystemTable()){

            Get get = new Get(table.getName());
            Result result = observerContext.getEnvironment().getTable(TableName.valueOf("table_view")).get(get);
            if(result != null && !result.isEmpty()){
                NavigableMap<byte[], byte[]> tableViewMap = result.getFamilyMap("views".getBytes());

                for (Map.Entry<byte[], byte[]> entry : tableViewMap.entrySet()) {
                    byte[] viewName = entry.getKey();
                    byte[] value = entry.getValue();

                    // TODO: Change this!!!
                    ViewManager.refreshView(new String(viewName));
                }
            }
        }






//        TableName table = observerContext.getEnvironment().getRegionInfo().getTable();
//        boolean errorFlag = false;
//        if(!table.isSystemTable()){
//
//            // Check if it is an updatable view table, unapdatable view table or base table,
//            Get get = new Get(table.getName());
//            Result result = observerContext.getEnvironment().getTable(TableName.valueOf("view_meta_data")).get(get);
//            if(result != null && !result.isEmpty()){ // View
//                if(result.getValue("settings".getBytes(), "updatable".getBytes()).toString().equals("u")){
//                    String type = result.getValue("settings".getBytes(), "type".getBytes()).toString();
//                    if(type.equals("select")){ // Updatable Select View
//                        // TODO: fill
//                    }
//                }else{
//                    return;
//                }
//            }else{ // Not View
//                result = observerContext.getEnvironment().getTable(TableName.valueOf("table_view")).get(get);
//                if(result != null && !result.isEmpty()){
//                    NavigableMap<byte[], byte[]> tableViewMap = result.getFamilyMap("views".getBytes());
//                    for (Map.Entry<byte[], byte[]> entry : tableViewMap.entrySet()) {
//                        byte[] viewName = entry.getKey();
//                        byte[] value = entry.getValue();
//
//                        // TODO: Change this!!!
//                        ViewManager.refreshView(new String(viewName));
//                    }
//                }
//            }
//        }
    }


    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put, WALEdit edit, Durability durability) throws IOException {
        TableName table = observerContext.getEnvironment().getRegionInfo().getTable();
        boolean errorFlag = false;
        if(!table.isSystemTable()){

            // Check if it is an updatable view table, unapdatable view table or base table,
            Get get = new Get(table.getName());
            Result result = observerContext.getEnvironment().getTable(TableName.valueOf("view_meta_data")).get(get);
            if(result != null && !result.isEmpty()){
                byte[] updatable = result.getValue("settings".getBytes(), "updatable".getBytes());
                if(updatable!= null && updatable.toString().equals("u")){
                    String type = result.getValue("settings".getBytes(), "type".getBytes()).toString();
                    if(type.equals("select")){
                        NavigableMap<byte[], byte[]> tableMap = result.getFamilyMap("tables".getBytes());

                        for (Map.Entry<byte[], byte[]> entry : tableMap.entrySet()) {
                            byte[] tableName = entry.getKey();

                            // TODO: Change this!!!
                            convertAndPut(put, tableName);
                        }
                        errorFlag = true;
                    }else{
//                        errorFlag = true;
                        return;
                    }
                }else{
                    return;
                }
            }
        }
        if(errorFlag){
            observerContext.complete();
            return;
        }
    }


    public static void convertAndPut(Put put, byte[] tableName){
//        HTable tbl = null;
//        try {
//            tbl = new HTable(HBaseConfiguration.create(), tableName);
//            tbl.put(put);
//            tbl.close();
//        } catch (IOException e) {
//            e.printStackTrace();
//        }






//        Configuration conf = HBaseConfiguration.create();
//        HBaseHelper helper;
//        put.get
//        try {
//            helper = HBaseHelper.getHelper(conf);
//            helper.put(tableName, put.getRow(), "colfam", put.get, "query", query);
//            helper.put(tableName, put.getRow(), "colfam", "settings", "type", type);
//            helper.put(tableName, put.getRow(), "colfam", "settings", "updatable", updatable);
//        } catch (IOException e) {
//            e.printStackTrace();
//        }
    }
}

