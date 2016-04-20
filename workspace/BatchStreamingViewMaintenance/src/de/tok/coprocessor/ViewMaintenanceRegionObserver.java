package de.tok.coprocessor;
import de.tok.sql.JsqlParser;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.*;

public class ViewMaintenanceRegionObserver extends BaseRegionObserver {

    ObserverContext<RegionCoprocessorEnvironment> oContext;

    private String viewQuery = null;
    private String viewName = null;
    private boolean successful = false;
    TableName modifiedTable;
    private boolean prePut = false;


    public void commonOperation(ObserverContext<RegionCoprocessorEnvironment> observerContext, Mutation op) throws IOException {
        modifiedTable = observerContext.getEnvironment().getRegionInfo().getTable();
        if (!modifiedTable.isSystemTable()) {
            oContext = observerContext;
            Get get = new Get(modifiedTable.getName());
            Result result = observerContext.getEnvironment().getTable(TableName.valueOf("table_view")).get(get);

            if (result != null && !result.isEmpty()) { // There exists a view connected to the table.
                NavigableMap<byte[], byte[]> tableViewMap = result.getFamilyMap("views".getBytes());

                for (Map.Entry<byte[], byte[]> entry : tableViewMap.entrySet()) {
                    byte[] viewName = entry.getKey();
                    byte[] query = entry.getValue();

                    // TODO:  Batch Processing.
//                    ViewManager.refreshView(new String(viewName));

                    // TODO:  Incremental Processing.

                    if (op instanceof Put) {
                        Put put = (Put) op;
                        setViewName(new String(viewName)).setViewQuery(new String(query)).execute(put);
                    } else if (op instanceof Delete) {
                        Delete delete = (Delete) op;
                        setViewName(new String(viewName)).setViewQuery(new String(query)).execute(delete);
                    }

                    if (!isSuccessful()) {
                        // TODO : LOG HERE
                    }
                }
            }
        }
    }

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put, WALEdit edit, Durability durability) throws IOException {
        prePut = false;
        commonOperation(observerContext, put);
    }

    @Override
    public void prePut(ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put, WALEdit edit, Durability durability) throws IOException {
        prePut = true;
        commonOperation(observerContext, put);
    }

    @Override
    public void preDelete(ObserverContext<RegionCoprocessorEnvironment> observerContext, Delete delete, WALEdit edit, Durability durability) throws IOException {
        commonOperation(observerContext, delete);
    }


    private ViewMaintenanceRegionObserver execute(Put put) throws IOException {
        if (viewQuery == null) return this;

        boolean errorFlag = false;
        byte viewType = JsqlParser.typeOfQuery(viewQuery);

        if (prePut == false && viewType == JsqlParser.SELECT) {
            this.select(put);
        } else if (viewType == JsqlParser.JOIN) {
            this.join(put);
        } else if (viewType == JsqlParser.AGGREGATION) {
            this.aggregation(put);
        } else {
            errorFlag = true;
        }
        successful = !errorFlag;
        return this;
    }

    public ViewMaintenanceRegionObserver execute(Delete delete) throws IOException {
        if (viewQuery == null) return this;

        boolean errorFlag = false;
        byte viewType = JsqlParser.typeOfQuery(viewQuery);

        if (viewType == JsqlParser.SELECT) {
            this.select(delete);
        } else if (viewType == JsqlParser.JOIN) {
            this.join(delete);
        } else if (viewType == JsqlParser.AGGREGATION) {
            this.aggregation(delete);
        } else {
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

    private boolean isSuccessful() {
        return successful;
    }

    private void select(Put put) throws IOException {
        Put viewPut = new Put(put.getRow());
        NavigableMap<byte[], List<Cell>> familyCellMap = put.getFamilyCellMap();
        for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
            List<Cell> cells = entry.getValue();
            for (Cell c : cells) {
                viewPut.add(CellUtil.cloneFamily(c), (new String(CellUtil.cloneQualifier(c))).getBytes(), CellUtil.cloneValue(c));
            }
        }
        HTableInterface view = oContext.getEnvironment().getTable(TableName.valueOf(viewName));

        view.put(viewPut);
        view.flushCommits();
        view.close();
    }

    private void select(Delete delete) throws IOException {
        deleteFromView(delete.getRow());
    }
    private void deleteFromView(byte[] row) throws IOException {
        Delete viewDelete = new Delete(row);
        HTableInterface view = oContext.getEnvironment().getTable(TableName.valueOf(viewName));
        view.delete(viewDelete);
        view.flushCommits();
        view.close();
    }

    private void join(Put put) throws IOException {
        if (prePut) {
            byte[] joinKey = findJoinKey(put);
            deleteFromJoinDeltaAndJoinView(joinKey, modifiedTable.getName(), put.getRow());
        } else {
            putToJoinDeltaAndJoinView(put);
        }

    }

    private String findJoinKeyQualifier() throws IOException {
        HTableInterface metaInterface = oContext.getEnvironment().getTable(TableName.valueOf("view_meta_data"));
        Result result = metaInterface.get(new Get(viewName.getBytes()));
        metaInterface.close();

        if (result != null && !result.isEmpty()) {
            return new String(result.getValue("tables".getBytes(), modifiedTable.getName()));
        }
        return null;
    }

    private void putToJoinDeltaAndJoinView(Put put) throws IOException {

        String joinKeyFamily = null;
        String joinKeyQualifier = null;
        String tmpStr = findJoinKeyQualifier();
        String[] tmp = tmpStr != null ? tmpStr.split("\\.") : new String[1];
        if (tmp.length < 2) return;
        joinKeyFamily = tmp[0];
        joinKeyQualifier = tmp[1];
        if (joinKeyFamily == null || joinKeyQualifier == null)
            return;

        HTableInterface deltaView = oContext.getEnvironment().getTable(TableName.valueOf(viewName + "_delta"));
        HTableInterface view = oContext.getEnvironment().getTable(TableName.valueOf(viewName));

        HTableInterface table = oContext.getEnvironment().getTable(modifiedTable);
        Result res = table.get(new Get(put.getRow()));
        table.close();

        byte[] baseTableName = modifiedTable.getName();

        if (res != null && !res.isEmpty()) {
            byte[] joinKey = res.getValue(joinKeyFamily.getBytes(), joinKeyQualifier.getBytes());
            if (joinKey == null)
                return; // TODO: Cleanup on returns.

            NavigableMap<byte[], byte[]> leftMap = res.getFamilyMap("colfam".getBytes());

            Result deltaResult = deltaView.get(new Get(joinKey));

            Put deltaViewPut = new Put(joinKey);
            // For left table
            for (Map.Entry<byte[], byte[]> el : leftMap.entrySet()) {
                byte[] leftQual = el.getKey(); // Qualifier Name.
                byte[] leftVal = el.getValue(); // Value
                deltaViewPut.add(baseTableName, (new String(put.getRow()) + "_" + new String(leftQual)).getBytes(), leftVal);
            }

            deltaView.put(deltaViewPut);

            NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = deltaResult.getNoVersionMap();
            // Map&family,Map<qualifier,value>>

            int i = 0;
            for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> e : map.entrySet()) {
                String family = new String(e.getKey()); //tableNames
                if (!family.equals(modifiedTable.getNameAsString())) {
                    for (Map.Entry<byte[], byte[]> e2 : e.getValue().entrySet()) {
                        byte[] serializedQual = e2.getKey(); // Qualifier Name.
                        String[] parseStr = (new String(serializedQual)).split("_");
                        if (parseStr.length < 2)
                            return;

                        String rightTableRowId = parseStr[0];
                        String rightQual = parseStr[1];
                        byte[] rightVal = e2.getValue();

                        String viewRowId;
                        if (i == 0) {
                            viewRowId = (new String(family) + "_" + rightTableRowId + "_" + new String(baseTableName) + "_" + (new String(put.getRow())));
                        } else {
                            viewRowId = (new String(baseTableName) + "_" + (new String(put.getRow())) + "_" + new String(family) + "_" + rightTableRowId);
                        }
                        Put viewPut = new Put(viewRowId.getBytes());
                        // For left table
                        for (Map.Entry<byte[], byte[]> el : leftMap.entrySet()) {
                            byte[] leftQual = el.getKey(); // Qualifier Name.
                            byte[] leftVal = el.getValue(); // Value
                            viewPut.add("colfam".getBytes(), leftQual, leftVal);
                        }
                        viewPut.add("colfam".getBytes(), rightQual.getBytes(), rightVal);
                        view.put(viewPut);
                    }
                }
                i++;
            }
        }


        deltaView.close();
        view.close();
    }

    private byte[] findJoinKey(Mutation operation) throws IOException {
        HTableInterface metaInterface = oContext.getEnvironment().getTable(TableName.valueOf("view_meta_data"));
        Result result = metaInterface.get(new Get(viewName.getBytes()));
        metaInterface.close();

        byte[] joinKey = null;

        if (result != null && !result.isEmpty()) {
            String[] tmp = (new String(result.getValue("tables".getBytes(), modifiedTable.getName()))).split("\\.");
            if (tmp.length < 2)
                return "joinkey".getBytes();
            String joinKeyFamily = tmp[0];
            String joinKeyQualifier = tmp[1];

            if (operation instanceof Delete) {
                NavigableMap<byte[], List<Cell>> familyCellMap = operation.getFamilyCellMap();
                for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
                    List<Cell> cells = entry.getValue();
                    for (Cell cell : cells) {
                        if ((new String(CellUtil.cloneQualifier(cell))).equals(joinKeyQualifier)) {
                            joinKey = CellUtil.cloneValue(cell);
                            break;
                        }
                    }
                    if (joinKey != null) {
                        break;
                    }
                }
            }

            if (joinKey == null) {
                HTableInterface table = oContext.getEnvironment().getTable(modifiedTable);
                joinKey = table.get(new Get(operation.getRow())).getValue(joinKeyFamily.getBytes(), joinKeyQualifier.getBytes());
                table.close();
            }
        }
        return joinKey;
    }

    private void join(Delete delete) throws IOException {
        byte[] joinKey = findJoinKey(delete);
        deleteFromJoinDeltaAndJoinView(joinKey, modifiedTable.getName(), delete.getRow());
    }

    private void deleteFromJoinDeltaAndJoinView(byte[] joinKey, byte[] baseTableName, byte[] deletedRowId) throws IOException {
        HTableInterface deltaView = oContext.getEnvironment().getTable(TableName.valueOf(viewName + "_delta"));
        HTableInterface view = oContext.getEnvironment().getTable(TableName.valueOf(viewName));

        if (joinKey != null) {

            NavigableMap<byte[], NavigableMap<byte[], byte[]>> map = deltaView.get(new Get(joinKey)).getNoVersionMap();
            // Map&family,Map<qualifier,value>>

            Delete deltaDelete = new Delete(joinKey);

            List<Delete> deleteList = new ArrayList<>();
            boolean deltaDeleteFlag = false;
            int i = 0;

            for (Map.Entry<byte[], NavigableMap<byte[], byte[]>> e : map.entrySet()) {
                String family = new String(e.getKey()); //tableNames

                for (Map.Entry<byte[], byte[]> e2 : e.getValue().entrySet()) {

                    byte[] deltaQual = e2.getKey(); // Qualifier Name.
                    String deltaRow = (new String(deltaQual)).split("_")[0];

                    if (family.equals(new String(baseTableName))) {
                        if ((new String(deletedRowId)).equals(deltaRow)) {
                            deltaDelete.deleteColumns(baseTableName, deltaQual);
                            deltaDeleteFlag = true;
                        } else {
                            continue;
                        }
                    } else {
                        String viewDeleteRowId;
                        if (i == 0) {
                            viewDeleteRowId = (new String(family) + "_" + deltaRow + "_" + new String(baseTableName) + "_" + (new String(deletedRowId)));
                        } else {
                            viewDeleteRowId = (new String(baseTableName) + "_" + (new String(deletedRowId)) + "_" + new String(family) + "_" + deltaRow);
                        }
                        Delete delete = new Delete(viewDeleteRowId.getBytes());
                        deleteList.add(delete);

                    }
                }
                ++i;
            }
            view.delete(deleteList);
            if (deltaDeleteFlag)
                deltaView.delete(deltaDelete);
        }

        deltaView.close();
        view.close();
    }

    private void aggregation(Put put) throws IOException {
        if (prePut) {
            // If aggregation key changed then delete!
            String groupBy = JsqlParser.getGroupBy(viewQuery);
            byte[] newAggKey = findNewAggregationKey(put, groupBy);
            if(newAggKey == null){
                return;
            }else{
                byte[] oldAggKey = findOldAggregationKey(put.getRow(), groupBy.getBytes());
                deleteFromAggregation(oldAggKey, put.getRow(), JsqlParser.typeOfAggregation(viewQuery));
            }
        } else {
            aggregation(put, JsqlParser.typeOfAggregation(viewQuery));
        }

    }

    private void aggregation(Put put, HashSet<Byte> aggSet) throws IOException {
        if (aggSet == null) return;
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
        if (result != null && !result.isEmpty()) {
            NavigableMap<byte[], byte[]> rowMap = result.getFamilyMap(colfam);

            for (Map.Entry<byte[], byte[]> entry : rowMap.entrySet()) {
                byte[] qual = entry.getKey();
                byte[] value = entry.getValue();

                if (groupBy.equals(new String(qual))) {
                    rowDeltaPrefix = "colfam.value." + new String(value);
                } else {
                    newValue = Integer.valueOf(new String(value));
                }
            }
        }
        table.close();

        Get getDelta = new Get(rowDeltaPrefix.getBytes());

        HTableInterface deltaView = oContext.getEnvironment().getTable(TableName.valueOf(viewName + "_delta"));
        result = deltaView.get(getDelta);

        if (result != null && !result.isEmpty()) {
            oldDeltaValue = result.getValue(colfam, put.getRow());

            MIN = Integer.valueOf((new String(result.getValue(colfam, "MIN".getBytes()))));
            MAX = Integer.valueOf((new String(result.getValue(colfam, "MAX".getBytes()))));
            SUM = Integer.valueOf((new String(result.getValue(colfam, "SUM".getBytes()))));
            COUNT = Integer.valueOf((new String(result.getValue(colfam, "COUNT".getBytes()))));
        }

        if (oldDeltaValue != null) {
            int oldValue = Integer.valueOf(new String(oldDeltaValue));

            SUM += (newValue - oldValue);

            if (newValue < MIN) {
                MIN = newValue;
            } else if (oldValue == MIN && newValue > MIN) {
                MIN = recalculateMin(result, put.getRow());
            }

            if (newValue > MAX) {
                MAX = newValue;
            } else if (oldValue == MAX && newValue < MAX) {
                MAX = recalculateMax(result, put.getRow());
            }
        } else {
            COUNT++;
            SUM += newValue;
            MIN = (newValue < MIN ? newValue : MIN);
            MAX = (newValue > MAX ? newValue : MAX);
        }


        deltaView.put(createDeltaViewPut(getDelta.getRow(), colfam, MIN, MAX, COUNT, SUM, put.getRow(), newValue));
        deltaView.flushCommits();
        deltaView.close();

        HTableInterface view = oContext.getEnvironment().getTable(TableName.valueOf(viewName));

        view.put(createViewPut(getDelta.getRow(), colfam, MIN, MAX, COUNT, SUM, aggSet));
        view.flushCommits();
        view.close();

    }

    private void aggregation(Delete delete) throws IOException {
        String groupBy = JsqlParser.getGroupBy(viewQuery);
        byte[] aggKey = findNewAggregationKey(delete, groupBy);
        if(aggKey == null){
            aggKey = findOldAggregationKey(delete.getRow(), groupBy.getBytes());;
        }


        deleteFromAggregation(aggKey, delete.getRow(), JsqlParser.typeOfAggregation(viewQuery));
    }

    private void deleteFromAggregation(byte[] aggKey, byte[] deleteRowId, HashSet<Byte> aggSet) throws IOException {
        if (aggSet == null) return;
        byte[] colfam = "colfam".getBytes();

        byte[] oldDeltaValue;

        int MIN, MAX, SUM, COUNT;

        String deltaViewRowId = (aggKey == null ? "colfam.value" : "colfam.value." + new String (aggKey));


        Get getDelta = new Get(deltaViewRowId.getBytes());

        HTableInterface deltaView = oContext.getEnvironment().getTable(TableName.valueOf(viewName + "_delta"));
        Result result = deltaView.get(getDelta);

        if (result != null && !result.isEmpty()) {
            Delete delete = new Delete(getDelta.getRow());

            if(result.getFamilyMap(colfam).size() > 5) {

                oldDeltaValue = result.getValue(colfam, deleteRowId);

                MIN = Integer.valueOf((new String(result.getValue(colfam, "MIN".getBytes()))));
                MAX = Integer.valueOf((new String(result.getValue(colfam, "MAX".getBytes()))));
                SUM = Integer.valueOf((new String(result.getValue(colfam, "SUM".getBytes()))));
                COUNT = Integer.valueOf((new String(result.getValue(colfam, "COUNT".getBytes()))));

                if (oldDeltaValue != null) {
                    int oldValue = Integer.valueOf(new String(oldDeltaValue));

                    SUM -= oldValue;

                    if (oldValue == MIN) {
                        MIN = recalculateMin(result, deleteRowId);
                    }

                    if (oldValue == MAX) {
                        MAX = recalculateMax(result, deleteRowId);
                    }

                    --COUNT;

                    delete.deleteColumns(colfam, deleteRowId);
                    deltaView.put(createDeltaViewPut(getDelta.getRow(), colfam, MIN, MAX, COUNT, SUM));

                    HTableInterface view = oContext.getEnvironment().getTable(TableName.valueOf(viewName));

                    view.put(createViewPut(getDelta.getRow(), colfam, MIN, MAX, COUNT, SUM, aggSet));
                    view.flushCommits();
                    view.close();
                }
            }else{
                deleteFromView(getDelta.getRow());
            }

            deltaView.delete(delete);
            deltaView.flushCommits();
            deltaView.close();

        }

    }

    private byte[] findOldAggregationKey(byte[] rowId, byte[] aggQualifier) throws IOException {
        byte[] aggKey;

        HTableInterface table = oContext.getEnvironment().getTable(modifiedTable);
        aggKey = table.get(new Get(rowId)).getValue("colfam".getBytes(), aggQualifier);
        table.close();

        return aggKey;
    }

    private byte[] findNewAggregationKey(Mutation operation, String aggQualifier){

        byte[] aggKey = null;

        NavigableMap<byte[], List<Cell>> familyCellMap = operation.getFamilyCellMap();
        for (Map.Entry<byte[], List<Cell>> entry : familyCellMap.entrySet()) {
            List<Cell> cells = entry.getValue();
            for (Cell cell : cells) {
                if ((new String(CellUtil.cloneQualifier(cell))).equals(aggQualifier)) {
                    aggKey = CellUtil.cloneValue(cell);
                    break;
                }
            }
            if (aggKey != null) {
                return aggKey;
            }
        }
        return aggKey;
    }

    private Put createDeltaViewPut(byte[] row, byte[] colfam, int MIN, int MAX, int COUNT, int SUM, byte[] putRow, int newValue){
        Put deltaPut = createDeltaViewPut(row, colfam, MIN, MAX, COUNT, SUM);
        deltaPut.add(colfam, putRow, String.valueOf(newValue).getBytes());
        return deltaPut;
    }
    private Put createDeltaViewPut(byte[] row, byte[] colfam, int MIN, int MAX, int COUNT, int SUM){
        Put deltaPut = new Put(row);
        deltaPut.add(colfam, "MIN".getBytes(), String.valueOf(MIN).getBytes());
        deltaPut.add(colfam, "MAX".getBytes(), String.valueOf(MAX).getBytes());
        deltaPut.add(colfam, "SUM".getBytes(), String.valueOf(SUM).getBytes());
        deltaPut.add(colfam, "COUNT".getBytes(), String.valueOf(COUNT).getBytes());
        return deltaPut;
    }
    private Put createViewPut(byte[] row, byte[] colfam, int MIN, int MAX, int COUNT, int SUM, HashSet<Byte> requiredAggFunctions){
//        HashSet<Byte> requiredAggFunctions = JsqlParser.typeOfAggregation(viewQuery);

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
