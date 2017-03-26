package ru.mipt;

import org.apache.hadoop.conf.*;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;

import java.io.IOException;

public class Map
        extends org.apache.hadoop.mapreduce.Mapper<LongWritable, Text, Text, Text> {
    @Override
    public void map(LongWritable key, Text value, Context context)
            throws IOException, InterruptedException {
        Configuration conf = context.getConfiguration();
        int m = Integer.parseInt(conf.get("m"));
        int p = Integer.parseInt(conf.get("p"));
        String line = value.toString();
        // (M, i, j, Mij);
        String[] indicesAndValue = line.split(",");
        Text outputKey = new Text();
        Text outputValue = new Text();
        String fileName = ((FileSplit) context.getInputSplit()).getPath().getName();
        if (fileName.contains("A")) {
            for (int k = 0; k < p; k++) {
                outputKey.set(indicesAndValue[0] + "," + k);
                // outputKey.set(i,k);
                outputValue.set("A" + "," + indicesAndValue[1]
                        + "," + indicesAndValue[2]);
                // outputValue.set(M,j,Mij);
                context.write(outputKey, outputValue);
            }
        } else {
            // (N, j, k, Njk);
            for (int i = 0; i < m; i++) {
                outputKey.set(i + "," + indicesAndValue[1]);
                outputValue.set("B" +  ","  + indicesAndValue[2]);
                context.write(outputKey, outputValue);
            }
        }
    }
}
