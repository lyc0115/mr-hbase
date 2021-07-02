package com.lyc.mrhbase.movehdfstohbase;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * @author lyc
 * @create 2021--07--01 9:43
 */
public class HdfsDataToHbaseMR extends Configured implements Tool {
    public int run(String[] strings) throws Exception {
        Configuration configuration = HBaseConfiguration.create();
        configuration.set("fs.defaultFS", "hdfs://hadoop101/");
        configuration.set("hbase.zookeeper.quorum", "hadoop101:2181,hadoop102:2181,hadoop103:2181");
        System.out.println(configuration);
        FileSystem fs = FileSystem.get(configuration);

        Job job = Job.getInstance(configuration);
        job.setJarByClass(HdfsDataToHbaseMR.class);
        job.setMapperClass(HdfsToHbaseMapper.class);
        job.setMapOutputKeyClass(Text.class);
        job.setMapOutputValueClass(NullWritable.class);

        TableMapReduceUtil.initTableReducerJob("student",
                HdfsToHbaseReducer.class,
                job,
                null,
                null,
                null,
                null,
                false);

        job.setOutputKeyClass(NullWritable.class);
        job.setOutputValueClass(Put.class);

        Path inputPath = new Path("/input");
        Path outputPath = new Path("/output");

        if (fs.exists(outputPath)) {
            fs.delete(outputPath, true);
        }

        FileInputFormat.addInputPath(job, inputPath);
        FileOutputFormat.setOutputPath(job, outputPath);

        boolean b = job.waitForCompletion(true);
        System.out.println(b ? 0 : 1);

        return 0;
    }

    public static void main(String[] args) throws Exception {
        int run = ToolRunner.run(new HdfsDataToHbaseMR(), args);
        System.exit(run);
    }
}
