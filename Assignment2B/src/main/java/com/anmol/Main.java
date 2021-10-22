package com.anmol;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.mapreduce.TableMapReduceUtil;

import org.apache.hadoop.mapreduce.Job;


import java.io.IOException;

import static com.anmol.util.Constants.*;

public class Main {
    public static void main(String[] args)
            throws IOException, InterruptedException, ClassNotFoundException {

        Configuration config = HBaseConfiguration.create();

        config.set(DEFAULT_FS, HDFS_INPUT_URL);

        Path output = new Path(ASSIGNMENT2_OUTPUT_PATH);
        FileSystem hdfs = FileSystem.get(config);

        // delete existing directory
        if (hdfs.exists(output)) {
            hdfs.delete(output, true);
        }

        Scan scan = new Scan();
        scan.setCaching(500);
        scan.setCacheBlocks(false);
        Job job = Job.getInstance(config);
        job.setJobName("MRTableReadWrite");
        job.setJarByClass(Main.class);

        TableMapReduceUtil.initTableMapperJob(
                PEOPLE_TABLE,
                scan,
                HbaseBulkLoadMapper.class,
                null,
                null,
                job
        );
        TableMapReduceUtil.initTableReducerJob(
                PEOPLE_BULK_UPLOAD_TABLE,      // output table
                null,                  // reducer class
                job
        );

        job.setNumReduceTasks(0);
        boolean b = job.waitForCompletion(true);
        System.out.println(b);

        if(job.isSuccessful()){
            System.out.println("Successful");
        }
    }
}
