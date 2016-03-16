package com.lin.coprocessor;
import com.lin.client.ViewManager;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.coprocessor.BaseRegionObserver;
import org.apache.hadoop.hbase.coprocessor.ObserverContext;
import org.apache.hadoop.hbase.coprocessor.RegionCoprocessorEnvironment;
import org.apache.hadoop.hbase.regionserver.wal.WALEdit;

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

    }

}

