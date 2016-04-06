package com.lin.coprocessor;
import com.lin.client.ViewManager;
import com.lin.sql.JsqlParser;
import com.lin.test.HBaseHelper;
import net.sf.jsqlparser.parser.JSqlParser;
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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;

public class ViewMaintenanceRegionObserver extends BaseRegionObserver{

    ObserverContext<RegionCoprocessorEnvironment> oContext;

    private String viewQuery = null;
    private String viewName = null;
    private boolean successful = false;
    TableName modifiedTable;



    public void commonOperation(ObserverContext<RegionCoprocessorEnvironment> observerContext, Mutation op) throws IOException{
        modifiedTable = observerContext.getEnvironment().getRegionInfo().getTable();
        if(!modifiedTable.isSystemTable()){
            oContext = observerContext;
            Get get = new Get(modifiedTable.getName());
            Result result = observerContext.getEnvironment().getTable(TableName.valueOf("table_view")).get(get);

            if(result != null && !result.isEmpty()){ // There exists a view connected to the table.
                NavigableMap<byte[], byte[]> tableViewMap = result.getFamilyMap("views".getBytes());

                for (Map.Entry<byte[], byte[]> entry : tableViewMap.entrySet()) {
                    byte[] viewName = entry.getKey();
                    byte[] query = entry.getValue();

                    // TODO:  Batch Processing.
//                    ViewManager.refreshView(new String(viewName));

                    // TODO:  Incremental Processing.
//                    HTableInterface deltaViewTable = observerContext.getEnvironment().getTable(TableName.valueOf(viewName + "_delta"));
//                    HTableInterface tableInterface = observerContext.getEnvironment().getTable(table);
                    if(op instanceof Put){
                        Put put = (Put) op;
                        setViewName(new String(viewName)).setViewQuery(new String(query)).execute(put);
                    }else if(op instanceof Delete){
                        Delete delete = (Delete) op;
                        setViewName(new String(viewName)).setViewQuery(new String(query)).execute(delete);
                    }

                    if(!isSuccessful()){
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


    private ViewMaintenanceRegionObserver execute(Put put) throws IOException {
        if(viewQuery == null) return this;

        boolean errorFlag = false;
        byte viewType = JsqlParser.typeOfQuery(viewQuery);

        if(viewType == JsqlParser.SELECT){
            this.select(put);
        }else if(viewType == JsqlParser.JOIN){
            this.join(put);
        }else if (viewType == JsqlParser.AGGREGATION){
            this.aggregation(put, JsqlParser.typeOfAggregation(viewQuery));
        }else{
            errorFlag = true;
        }
        successful = !errorFlag;
        return this;
    }

    public ViewMaintenanceRegionObserver execute(Delete delete) throws IOException {
        if(viewQuery == null) return this;

        boolean errorFlag = false;
        byte viewType = JsqlParser.typeOfQuery(viewQuery);

        if(viewType == JsqlParser.SELECT){
            this.select(delete);

        }else if(viewType == JsqlParser.JOIN){


        }else if (viewType == JsqlParser.AGGREGATION){


        }else{
            errorFlag = true;
        }
        successful = !errorFlag;
        return this;
    }



    public ViewMaintenanceRegionObserver setViewName(String viewName) {
        this.viewName = viewName;
        return this;
    }

    public ViewMaintenanceRegionObserver setViewQuery(String viewQuery) {
        this.viewQuery = viewQuery;
        return this;
    }
    private boolean isSuccessful(){
        return successful;
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
        HTableInterface view = oContext.getEnvironment().getTable(TableName.valueOf(viewName));

        view.put(viewPut);
        view.flushCommits();
        view.close();
    }

    private void select(Delete delete) throws IOException {
        Delete viewDelete = new Delete(delete.getRow());
        NavigableMap<byte[], List<Cell>> familyCellMap = delete.getFamilyCellMap();
        for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
            List<Cell> cells = entry.getValue();
            for(Cell c: cells){
                viewDelete.deleteColumn(CellUtil.cloneFamily(c), (new String(CellUtil.cloneQualifier(c))).getBytes());
            }
        }

        HTableInterface view = oContext.getEnvironment().getTable(TableName.valueOf(viewName));
        view.delete(viewDelete);
        view.flushCommits();
        view.close();
    }

    private void join(Put put){

    }


    private void aggregation(Put put, HashSet<Byte> aggSet) throws IOException {
        if(aggSet == null) return;
        byte[] colfam = "colfam".getBytes();

        byte[] oldDeltaValue = null;

        int MIN = Integer.MAX_VALUE;
        int MAX = Integer.MIN_VALUE;
        int SUM = 0;
        int COUNT = 0;

        int newValue = 0;

        String groupBy = JsqlParser.getGroupBy(viewQuery);
//        String groupBy = "aggKey";
        String rowDeltaPrefix = "colfam.value";


        HTableInterface table = oContext.getEnvironment().getTable(modifiedTable);
        Result result = table.get(new Get(put.getRow()));
        if(result != null && !result.isEmpty()) {
            NavigableMap<byte[], byte[]> rowMap = result.getFamilyMap(colfam);

            for (Map.Entry<byte[], byte[]> entry : rowMap.entrySet()) {
                byte[] qual = entry.getKey();
                byte[] value = entry.getValue();

                if(groupBy.equals(new String(qual))){
                    rowDeltaPrefix = "colfam.value." + new String(value);
                }else{ // TODO : Check if it is the value or if there are more values.
                    newValue = Integer.valueOf(new String(value));
                }
            }
        }
        table.close();

        Get getDelta = new Get(rowDeltaPrefix.getBytes());

        HTableInterface deltaView = oContext.getEnvironment().getTable(TableName.valueOf(viewName + "_delta"));
        result = deltaView.get(getDelta);

        if(result != null && !result.isEmpty()){
            oldDeltaValue = result.getValue(colfam, put.getRow());

            MIN = Integer.valueOf((new String(result.getValue(colfam, "MIN".getBytes()))));
            MAX = Integer.valueOf((new String(result.getValue(colfam, "MAX".getBytes()))));
            SUM = Integer.valueOf((new String(result.getValue(colfam, "SUM".getBytes()))));
            COUNT = Integer.valueOf((new String(result.getValue(colfam, "COUNT".getBytes()))));
        }

        if(oldDeltaValue != null){
            int oldValue = Integer.valueOf(new String(oldDeltaValue));

            SUM += (newValue - oldValue);

            if(newValue < MIN){
                MIN = newValue;
            }else if(oldValue == MIN && newValue > MIN){
                // TODO : Recalculate..
                MIN = recalculateMin(result, put.getRow());
            }

            if(newValue > MAX){
                MAX = newValue;
            }else if(oldValue == MAX && newValue < MAX){
                // TODO : Recalculate..
                MAX = recalculateMax(result, put.getRow());
            }
        }else{
            COUNT++;
            SUM += newValue;
            MIN = (newValue < MIN ? newValue : MIN);
            MAX = (newValue > MAX ? newValue : MAX);
        }


        deltaView.put(createDeltaViewPut(getDelta.getRow(), colfam, MIN, MAX, COUNT, SUM, put.getRow(), newValue));
        deltaView.flushCommits();
        deltaView.close();

        HTableInterface view = oContext.getEnvironment().getTable(TableName.valueOf(viewName));

        view.put(createViewPut(getDelta.getRow(), colfam, MIN, MAX, COUNT, SUM));
        view.flushCommits();
        view.close();

    }
    private Put createDeltaViewPut(byte[] row, byte[] colfam, int MIN, int MAX, int COUNT, int SUM, byte[] putRow, int newValue){
        Put deltaPut = new Put(row);
        deltaPut.add(colfam, putRow, String.valueOf(newValue).getBytes());
        deltaPut.add(colfam, "MIN".getBytes(), String.valueOf(MIN).getBytes());
        deltaPut.add(colfam, "MAX".getBytes(), String.valueOf(MAX).getBytes());
        deltaPut.add(colfam, "SUM".getBytes(), String.valueOf(SUM).getBytes());
        deltaPut.add(colfam, "COUNT".getBytes(), String.valueOf(COUNT).getBytes());
        return deltaPut;
    }
    private Put createViewPut(byte[] row, byte[] colfam, int MIN, int MAX, int COUNT, int SUM){
        HashSet<Byte> requiredAggFunctions = JsqlParser.typeOfAggregation(viewQuery);

        Put viewPut = new Put(row);
        if(requiredAggFunctions.contains(JsqlParser.AGGREGATION_MIN)) viewPut.add(colfam, "MIN".getBytes(), String.valueOf(MIN).getBytes());
        if(requiredAggFunctions.contains(JsqlParser.AGGREGATION_MAX)) viewPut.add(colfam, "MAX".getBytes(), String.valueOf(MAX).getBytes());
        if(requiredAggFunctions.contains(JsqlParser.AGGREGATION_SUM)) viewPut.add(colfam, "SUM".getBytes(), String.valueOf(SUM).getBytes());
        if(requiredAggFunctions.contains(JsqlParser.AGGREGATION_COUNT)) viewPut.add(colfam, "COUNT".getBytes(), String.valueOf(COUNT).getBytes());
        if(requiredAggFunctions.contains(JsqlParser.AGGREGATION_AVG)) viewPut.add(colfam, "AVG".getBytes(), String.valueOf(1.0 * SUM / COUNT).getBytes());
        return viewPut;
    }
    private int recalculateMin(Result result, byte[] oldRowId){
        if(result == null) return Integer.MAX_VALUE;
        String oldRowIdStr = new String(oldRowId);
        int min = Integer.MAX_VALUE;

        NavigableMap<byte[], byte[]> qualValueMap = result.getFamilyMap("colfam".getBytes());

        for (Map.Entry<byte[], byte[]> entry : qualValueMap.entrySet()) {
            byte[] qual = entry.getKey();
            byte[] value = entry.getValue();
            String qualStr = new String(qual);
            if(isAggregation(qualStr) || qualStr.equals(oldRowIdStr)){
                continue;
            }
            int val = Integer.valueOf(new String(value));
            if(val < min){
                min = val;
            }
        }
        return min;
    }
    private int recalculateMax(Result result, byte[] oldRowId){
        if(result == null) return Integer.MIN_VALUE;
        String oldRowIdStr = new String(oldRowId);
        int max = Integer.MIN_VALUE;

        NavigableMap<byte[], byte[]> qualValueMap = result.getFamilyMap("colfam".getBytes());

        for (Map.Entry<byte[], byte[]> entry : qualValueMap.entrySet()) {
            byte[] qual = entry.getKey();
            byte[] value = entry.getValue();
            String qualStr = new String(qual);
            if(isAggregation(qualStr) || qualStr.equals(oldRowIdStr)){
                continue;
            }
            int val = Integer.valueOf(new String(value));
            if(val > max){
                max = val;
            }
        }
        return max;
    }

    boolean isAggregation(String str){
        if(str.equals("MIN")) return true;
        else if(str.equals("MAX")) return true;
        else if(str.equals("SUM")) return true;
        else if(str.equals("COUNT")) return true;
        else if(str.equals("AVG")) return true;
        else return false;
    }
}

