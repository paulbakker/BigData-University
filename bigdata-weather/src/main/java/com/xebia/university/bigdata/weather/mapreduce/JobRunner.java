package com.xebia.university.bigdata.weather.mapreduce;

import static java.lang.System.*;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

import com.xebia.university.bigdata.weather.mapreduce.rain.AverageDailyPrecipitationPerMonth;


public class JobRunner extends Configured implements Tool {

    public static void main(String[] args) throws Exception {
        System.exit(ToolRunner.run(new JobRunner(), args));
    }

    @Override
    public int run(String[] args) throws Exception {
        if (args.length != 3) {
            printUsageInfo();
            return 1;
        }

        Job job = AverageDailyPrecipitationPerMonth.createJob(getConf(), new Path(args[1]), new Path(args[2]));

        return job.waitForCompletion(true) ? 0 : 1;
    }

    private void printUsageInfo() {
        out.println("Usage: hadoop jar <jar containing this class> <input> <output>");
        out.println("  where:");
        out.println("  - <input>  is the input file name or a glob pattern describing a group of input files.");
        out.println("  - <output> is the output file name on hdfs.");
        out.println();

        ToolRunner.printGenericCommandUsage(err);
    }}