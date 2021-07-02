package com.lyc.mrhbase.movehbasetohbase;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapper;
import org.apache.hadoop.hbase.util.Bytes;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--07--01 13:56
 */
public class HbaseToHbaseMapper extends TableMapper<ImmutableBytesWritable, Put> {

    @Override
    protected void map(ImmutableBytesWritable key, Result value, Context context) throws IOException, InterruptedException {
        //1.读取数据，拿到一条rowkey的值
        Put put = new Put(key.get());
        for (Cell cell : value.rawCells()) {
            if ("info".equals(Bytes.toString(CellUtil.cloneFamily(cell)))) {
                if ("name".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                } else if ("age".equals(Bytes.toString(CellUtil.cloneQualifier(cell)))) {
                    put.add(cell);
                }
            }
        }

        context.write(key,put);
    }
}
