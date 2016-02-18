package com.lin.coprocessor;
import com.lin.sql.ViewManager;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Durability;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

import java.io.IOException;
import java.util.Map;
import java.util.NavigableMap;

public class PutOperationObserver extends BaseRegionObserver{

    @Override
    public void postPut(ObserverContext<RegionCoprocessorEnvironment> observerContext, Put put, WALEdit edit, Durability durability) throws IOException {
        Get get = new Get(observerContext.getEnvironment().getRegion().getRegionInfo().getTable().getName());
        Result result = observerContext.getEnvironment().getTable(TableName.valueOf("table_view")).get(get);
        NavigableMap<byte[], byte[]> tableViewMap = result.getFamilyMap("views".getBytes());

        for (Map.Entry<byte[], byte[]> entry : tableViewMap.entrySet()) {
            byte[] viewName = entry.getKey();
            byte[] value = entry.getValue();

            // TODO: Change this!!!
            ViewManager.refreshView(new String(viewName));
        }
    }
}