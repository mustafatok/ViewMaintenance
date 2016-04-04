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

    public void commonOperation(ObserverContext<RegionCoprocessorEnvironment> observerContext, Mutation op) throws IOException{
        TableName table = observerContext.getEnvironment().getRegionInfo().getTable();
        if(!table.isSystemTable()){

            Get get = new Get(table.getName());
            Result result = observerContext.getEnvironment().getTable(TableName.valueOf("table_view")).get(get);

            if(result != null && !result.isEmpty()){ // There exists a view connected to the table.
                NavigableMap<byte[], byte[]> tableViewMap = result.getFamilyMap("views".getBytes());

                for (Map.Entry<byte[], byte[]> entry : tableViewMap.entrySet()) {
                    byte[] viewName = entry.getKey();
                    byte[] query = entry.getValue();

                    // TODO:  Batch Processing.
//                    ViewManager.refreshView(new String(viewName));

                    // TODO:  Incremental Processing.
                    HTableInterface viewTable = observerContext.getEnvironment().getTable(TableName.valueOf(viewName));
                    HTableInterface tableInterface = observerContext.getEnvironment().getTable(table);
                    Command command = new Command();
                    if(op instanceof Put){
                        Put put = (Put) op;
                        command.addTable(tableInterface).addViewTable(viewTable).addViewQuery(new String(query)).execute(put);
                    }else if(op instanceof Delete){
                        Delete delete = (Delete) op;
                        command.addTable(tableInterface).addViewTable(viewTable).addViewQuery(new String(query)).execute(delete);
                    }

                    if(!command.isSuccessful()){
                        // TODO : LOG HERE
                    }
                }
            }
        }
    }
    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put, WALEdit edit, Durability durability) throws IOException {
        commonOperation(observerContext, put);
    }

    @Override
    public void postDelete(ObserverContext<RegionCoprocessorEnvironment> observerContext, Delete delete, WALEdit edit, Durability durability) throws IOException {
        commonOperation(observerContext, delete);
    }











    public class Command {
        String viewQuery;
        boolean successful = false;
        private HTableInterface view = null;
        private HTableInterface table = null;

        public Command addTable(HTableInterface table){
            this.table = table;
            return this;
        }
        public Command addViewTable(HTableInterface view){
            this.view = view;
            return this;
        }
        public Command addViewQuery(String query){
            this.viewQuery = query;
            return this;
        }
        private void select(Put put) throws IOException {
            Put viewPut = new Put(put.getRow());
            NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
            for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
                List<Cell> cells = entry.getValue();
                for(Cell c: cells){
                    viewPut.add(CellUtil.cloneFamily(c), (new String(CellUtil.cloneQualifier(c))).getBytes(), CellUtil.cloneValue(c));
                }
            }
            view.put(viewPut);
            view.flushCommits();
        }
        private void join(Put put){

        }


        // TODO: Create delta views.
        private void aggregation(Put put, String aggType) throws IOException {
            Result result = table.get(new Get(put.getRow()));
            if(result != null && !result.isEmpty()){
                NavigableMap<byte[], NavigableMap<byte[], NavigableMap<Long,byte[]>>> tableViewMap = result.getMap();
//                    Map&family,Map<qualifier,Map<timestamp,value>>>


                long curr_ts = put.getTimeStamp();
                long tsdiff = Long.MAX_VALUE;
                long prev_val = 0;

                for (Map.Entry<byte[], NavigableMap<byte[], NavigableMap<Long,byte[]>>> entry1 : tableViewMap.entrySet()) {
                    for (Map.Entry<byte[], NavigableMap<Long,byte[]>> entry2 : entry1.getValue().entrySet()) {
                        String qual = new String(entry2.getKey());

                        if(!qual.equals("value")) continue;
                        for (Map.Entry<Long,byte[]> entry3 : entry2.getValue().entrySet()) {
                            Long ts = entry3.getKey();
                            byte[] val = entry3.getValue();
                            if(curr_ts - ts < tsdiff && curr_ts - ts != 0){
                                tsdiff = curr_ts - ts;
                                prev_val = Long.valueOf(new String(val)).longValue();
                            }
                        }
                    }
                }

                // TODO : Check previous MAX and MIN for estimations..

                long curr_val;
                curr_val = Long.valueOf(
                        new String(
                                CellUtil.cloneValue(
                                        put.get("colfam".getBytes(),"value".getBytes()).get(0)
                                )
                        )

                );


                byte[] viewKey = ("colfam.value." + new String(result.getValue("colfam".getBytes(),"aggKey".getBytes()))).getBytes();

                result = view.get(new Get((viewKey)));
                if(result != null && !result.isEmpty()){
                    if(aggType.equals("max")){
                        long viewMax = Long.valueOf(new String(result.getValue("colfam".getBytes(), "MAX".getBytes())));
                        if(viewMax < curr_val){
                            // Update View
                            Put viewPut = new Put(viewKey);
                            viewPut.add("colfam".getBytes(), "MAX".getBytes(), Long.toString(curr_val).getBytes());
                            view.put(viewPut);
                            view.flushCommits();
                        }

                    }else if(aggType.equals("min")){
                        long viewMin = Long.valueOf(new String(result.getValue("colfam".getBytes(), "MIN".getBytes())));
                        if(viewMin > curr_val){
                            // Update View
                            Put viewPut = new Put(viewKey);
                            viewPut.add("colfam".getBytes(), "MIN".getBytes(), Long.toString(curr_val).getBytes());
                            view.put(viewPut);
                            view.flushCommits();
                        }
                    }else if(aggType.equals("count")){
                        if(tsdiff == Long.MAX_VALUE){
//                                view.incrementColumnValue(viewKey, "colfam".getBytes(), "COUNT".getBytes(), 1);
                            long viewCnt = Long.valueOf(new String(result.getValue("colfam".getBytes(), "COUNT".getBytes())));
                            // Update View
                            Put viewPut = new Put(viewKey);
                            viewPut.add("colfam".getBytes(), "COUNT".getBytes(), Long.toString(viewCnt + 1).getBytes());
                            view.put(viewPut);
                            view.flushCommits();
                        }
                    }else if(aggType.equals("sum")){
//                            view.incrementColumnValue(viewKey, "colfam".getBytes(), "SUM".getBytes(), curr_val - prev_val); // Gave an error

                        long viewSum = Long.valueOf(new String(result.getValue("colfam".getBytes(), "SUM".getBytes())));
                        // Update View
                        Put viewPut = new Put(viewKey);
                        viewPut.add("colfam".getBytes(), "SUM".getBytes(), Long.toString(curr_val - prev_val).getBytes());
//                            viewPut.add("colfam".getBytes(), "SUM".getBytes(), Long.toString(viewSum + (curr_val - prev_val)).getBytes());
                        view.put(viewPut);
                        view.flushCommits();




                    }else if(aggType.equals("avg")){
//                            Long viewMax = new Long(result.getValue("colfam".getBytes(), "AVG".getBytes()).toString());

                    }else{

                    }
                }

            }
        }
        public Command execute(Put put) throws IOException {
            if(view == null) return this;

            boolean errorFlag = false;
            String viewType = JsqlParser.typeOfQuery(viewQuery);

            if(viewType.equals("select")){
                this.select(put);
            }else if(viewType.equals("join")){
                this.join(put);
            }else if (viewType.equals("aggregation")){
                this.aggregation(put, JsqlParser.typeOfAggregation(viewQuery));
            }else{
                errorFlag = true;
            }
            successful = !errorFlag;
            return this;
        }


        public Command execute(Delete delete) throws IOException {
            if(view == null) return this;

            boolean errorFlag = false;
            String viewType = JsqlParser.typeOfQuery(viewQuery);

            if(viewType.equals("select")){
                Delete viewDelete = new Delete(delete.getRow());
                NavigableMap<byte[], List<Cell>> familyCellMap = delete.getFamilyCellMap();
                for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
                    List<Cell> cells = entry.getValue();
                    for(Cell c: cells){
                        viewDelete.deleteColumn(CellUtil.cloneFamily(c), (new String(CellUtil.cloneQualifier(c))).getBytes());
                    }
                }
                view.delete(viewDelete);
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

