package com.anmol;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import static com.anmol.util.Constants.*;

public class HbaseBulkLoadMapper extends TableMapper<ImmutableBytesWritable, Put>{


    public static ImmutableBytesWritable newTable = new ImmutableBytesWritable(Bytes.toBytes(PEOPLE_BULK_UPLOAD_TABLE));

    public HbaseBulkLoadMapper(){
        System.out.println("Reached Mapper");
    }

    public void map(ImmutableBytesWritable key, Result result, Context context) {
        try {
            Put put = new Put(key.get());
            for (Cell cell : result.listCells()) {
                System.out.println(cell);
                put.addColumn(CellUtil.cloneFamily(cell), CellUtil.cloneQualifier(cell), CellUtil.cloneValue(cell));
            }
            context.write(newTable, put);
        } catch(Exception exception) {
            exception.printStackTrace();
        }
    }
}