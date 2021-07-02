package com.lyc.mrhbase.movehbasetohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author lyc
 * @create 2021--07--01 13:51
 */
public class HbaseToHbaseDriver extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        Configuration conf = HBaseConfiguration.create();
        conf.set("fs.defaultFS", "hdfs://hadoop101/");
        conf.set("hbase.zookeeper.quorum", "hadoop101:2181,hadoop102:2181,hadoop103:2181");

        Job job = Job.getInstance(conf);

        job.setJarByClass(HbaseToHbaseDriver.class);

        TableMapReduceUtil.initTableMapperJob("student", new Scan(), HbaseToHbaseMapper.class,
                ImmutableBytesWritable.class, Put.class, job);

        TableMapReduceUtil.initTableReducerJob("teacher", HbaseToHbaseReducer.class, job);
        boolean result = job.waitForCompletion(true);
        return result ? 0 : 1;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new HbaseToHbaseDriver(), args);
        System.exit(run);
    }
}
