package com.lyc.mrhbase.movehdfstohbase;

import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableReducer;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;

import java.io.IOException;

/**
 * @author lyc
 * @create 2021--07--01 9:54
 */
public class HdfsToHbaseReducer extends TableReducer<Text, NullWritable, NullWritable> {
    @Override
    protected void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException,
            InterruptedException {
        String[] split = key.toString().split(",");
        Put put = new Put(Bytes.toBytes(split[0]));

        put.addColumn(Bytes.toBytes("info"), Bytes.toBytes("name"), Bytes.toBytes(split[1]))
                .addColumn(Bytes.toBytes("info"), Bytes.toBytes("sex"), Bytes.toBytes(split[2]))
                .addColumn(Bytes.toBytes("info"), Bytes.toBytes("age"), Bytes.toBytes(split[3]))
                .addColumn(Bytes.toBytes("info"), Bytes.toBytes("department"), Bytes.toBytes(split[4]));

        context.write(NullWritable.get(),put);
    }
}
