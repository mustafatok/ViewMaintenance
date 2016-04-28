package de.tok.sql;

import de.tok.utils.HBaseHelper;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Delete;

import java.io.IOException;
import java.util.Arrays;

public class DeleteElement extends LogicalElement {
    private String tableName = null;
    byte[] row;

    @Override
    public void execute() {
        try {
            HBaseHelper.getHelper(HBaseConfiguration.create()).delete(tableName, generateDelete());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    Delete generateDelete(){
        Delete delete = new Delete(row);
        return delete;
    }

    public byte[] getRow() {
        return row;
    }

    public void setRow(byte[] row) {
        this.row = row;
    }

    @Override
    public String toString() {
        return "DeleteElement{" +
                "tableName='" + tableName + '\'' +
                ", row=" + Arrays.toString(row) +
                '}';
    }
}
