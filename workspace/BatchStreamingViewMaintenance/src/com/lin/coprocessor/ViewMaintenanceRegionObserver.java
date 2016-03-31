package com.lin.coprocessor;
import com.lin.client.ViewManager;
import com.lin.sql.JsqlParser;
import com.lin.test.HBaseHelper;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class ViewMaintenanceRegionObserver extends BaseRegionObserver{

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put, WALEdit edit, Durability durability) throws IOException {
        TableName table = observerContext.getEnvironment().getRegionInfo().getTable();
        if(!table.isSystemTable()){

            Get get = new Get(table.getName());
            Result result = observerContext.getEnvironment().getTable(TableName.valueOf("table_view")).get(get);
            if(result != null && !result.isEmpty()){ // There exists a view connected to the table.
                NavigableMap<byte[], byte[]> tableViewMap = result.getFamilyMap("views".getBytes());

                for (Map.Entry<byte[], byte[]> entry : tableViewMap.entrySet()) {
                    byte[] viewName = entry.getKey();
                    byte[] query = entry.getValue();

                    // TODO: Change this!!! -- Batch Processing.
//                    ViewManager.refreshView(new String(viewName));

                    // TODO: Change this!! -- Incremental Processing.
                    HTableInterface viewTable = observerContext.getEnvironment().getTable(TableName.valueOf(viewName));
                    PutCommand command = (new PutCommand()).addTableName(table.getName()).addViewTable(viewTable).addPut(put).addQuery(new String(query)).execute();

                    if(!command.isSuccessful()){
                        // TODO : LOG HERE
                    }
                }
            }
        }
    }

    public class PutCommand {
        byte[] tableName;
        Put put;
        String query;
        boolean successful = false;
        private HTableInterface view = null;

        public PutCommand addTableName(byte[] tableName){
            this.tableName = tableName;
            return this;
        }
        public PutCommand addViewTable(HTableInterface view){
            this.view = view;
            return this;
        }
        public PutCommand addPut(Put put){
            this.put = put;
            return this;
        }
        public PutCommand addQuery(String query){
            this.query = query;
            return this;
        }
        public PutCommand execute() throws IOException {
            if(view == null) return this;

            boolean errorFlag = false;
            String viewType = JsqlParser.typeOfQuery(query);

            if(viewType.equals("select")){
                Put viewPut = new Put(put.getRow());
                put.getFamilyCellMap();
                NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
                for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
                    List<Cell> cells = entry.getValue();
                    for(Cell c: cells){
                        viewPut.add(CellUtil.cloneFamily(c), (new String(CellUtil.cloneQualifier(c)) + "_new").getBytes(), CellUtil.cloneValue(c));
                    }
                }
                view.put(viewPut);
                view.flushCommits();

            }else if(viewType.equals("join")){


            }else if (viewType.equals("aggregation")){


            }else{
                errorFlag = true;
            }
            successful = !errorFlag;
            return this;
        }
        public boolean isSuccessful(){
            return successful;
        }
    }
}

