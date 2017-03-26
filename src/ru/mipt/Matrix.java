package ru.mipt;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.util.ToolRunner;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.reduce.IntSumReducer;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.lang.InterruptedException;
import java.io.IOException;
import java.util.*;

public class Matrix extends Configured implements Tool{

    public static void main(String[] args) throws Exception {
        ToolRunner.run(new Matrix(), args);
    }

    @Override
    public int run(String[] args) throws Exception {
        Configuration conf = this.getConf();
        FileSystem fs = FileSystem.get(conf);
        Path inputFilePath1 = new Path(args[0]);
        Path inputFilePath2 = new Path(args[1]);
        Path outputFilePath = new Path(args[2]);
        if (fs.exists(outputFilePath)) {
            fs.delete(outputFilePath, true);
        }
        conf.set("m", "5000");
        conf.set("n", "20000");
        conf.set("p", "2000");
        Job job = new Job(conf);
        job.setJarByClass(Matrix.class);
        job.setOutputKeyClass(Text.class);
        job.setOutputValueClass(Text.class);

        job.setMapperClass(Map.class);
        job.setReducerClass(Reduce.class);
        job.setNumReduceTasks(2);

        job.setInputFormatClass(TextInputFormat.class);
        job.setOutputFormatClass(TextOutputFormat.class);

        FileInputFormat.addInputPath(job, inputFilePath1);
        FileInputFormat.addInputPath(job, inputFilePath2);
        FileOutputFormat.setOutputPath(job, outputFilePath);

        job.waitForCompletion(true);
        for (FileStatus status: fs.listStatus(outputFilePath)){
            Scanner scanner = new Scanner(fs.open(status.getPath()));
            while(scanner.hasNextLine()){
                String line = scanner.nextLine();
                String[] split = line.split(",");
                if (split[0].equals("19")&&split[1].equals("17"))
                    System.out.println(split[2]);
            }
        }

        return 0;
    }
}
