package Statistic.Statistic;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;

import Statistic.Statistic.StatisticIP.MapFirst;
import Statistic.Statistic.StatisticIP.ReduceFirst;

public class StatisticDir {

	public static class MapFirst extends Mapper<Object,Text,Text,IntWritable>{
    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	
    	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
    		String line = value.toString();
    		String Ipstr = line.split(" ")[6];
    		word.set(Ipstr);
    		context.write(word, one);
    	}
    }
    
    public static class ReduceFirst extends Reducer<Text,IntWritable,Text,IntWritable>{
    	private IntWritable result = new IntWritable();
    	
    	public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException{
    		int sum = 0;
    		for(IntWritable val : values){
    			sum += val.get();
    		}
    		result.set(sum);
    		context.write(key, result);
    	}
    }
    
    private static void printUsage()
    {
      System.err.println("Usage : StatisticDir <input> <output>");
    }
    
	public static void main(String[] args) throws IOException, ClassNotFoundException, InterruptedException {
		Configuration conf = new Configuration();
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	if(otherArgs.length < 2){
    		printUsage();
    		System.exit(2);
    	}
    	Job job = new Job(conf, "statisticdir");
    	job.setJarByClass(StatisticDir.class);
    	job.setMapperClass(MapFirst.class);
    	job.setCombinerClass(ReduceFirst.class);
    	job.setReducerClass(ReduceFirst.class);
    	job.setOutputKeyClass(Text.class);
    	job.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job, new Path(args[1]));
    	
    	System.exit(job.waitForCompletion(true) ? 0 : 1);
	  }


}
