package Statistic.Statistic;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.GenericOptionsParser;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * StatisticIP
 *
 */
public class StatisticIP implements Tool
{
    public static class MapFirst extends Mapper<Object,Text,Text,IntWritable>{
    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text();
    	
    	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
    		String line = value.toString();
    		String Ipstr = line.split(" ")[0];
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
    
    public static class MapSecond extends Mapper<Object,Text,Text,IntWritable>{
    	private final static IntWritable one = new IntWritable(1);
    	private Text word = new Text("TotalIP");
    	
    	public void map(Object key,Text value,Context context) throws IOException, InterruptedException{
    		context.write(word, one);
    	}
    }
    
    public static class ReduceSecond extends Reducer<Text,IntWritable,Text,IntWritable>{
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
    
    private void printUsage()
    {
      System.err.println("Usage : StatisticIP <input> <output>");
    }
    
    public int run(String[] args) throws IOException, ClassNotFoundException, InterruptedException{
    	
    	Configuration conf = new Configuration();
    	String[] otherArgs = new GenericOptionsParser(conf, args).getRemainingArgs();
    	if(otherArgs.length < 2){
    		printUsage();
    		System.exit(2);
    	}
    	Path tempDir = new Path("temp-"+Integer.toString(new Random().nextInt(2147483647)));
    	
    	Job job1 = new Job(conf, "job1");
    	job1.setJarByClass(StatisticIP.class);
    	job1.setMapperClass(MapFirst.class);
    	job1.setCombinerClass(ReduceFirst.class);
    	job1.setReducerClass(ReduceFirst.class);
    	job1.setOutputKeyClass(Text.class);
    	job1.setOutputValueClass(IntWritable.class);
    	FileInputFormat.addInputPath(job1, new Path(args[0]));
    	FileOutputFormat.setOutputPath(job1, tempDir);
    	
    	job1.waitForCompletion(true);
    	
    	Job job2 = new Job(conf,"job2");
    	job2.setJarByClass(StatisticIP.class);
    	job2.setMapperClass(MapSecond.class);
    	job2.setCombinerClass(ReduceSecond.class);
    	job2.setReducerClass(ReduceSecond.class);
    	job2.setOutputKeyClass(Text.class);
    	job2.setOutputValueClass(IntWritable.class);
    	
    	FileInputFormat.setInputPaths(job2, new Path[] { tempDir });
    	FileOutputFormat.setOutputPath(job2, new Path(args[1]));
    	job2.waitForCompletion(true);
    	
    	FileSystem.get(conf).delete(tempDir, true);
    	return 0;
    }
    public static void main(String[] args) throws Exception{
    	int res = ToolRunner.run(new Configuration(), new StatisticIP(), args);
        System.exit(res);
    }

	public Configuration getConf() {
		// TODO Auto-generated method stub
		return null;
	}

	public void setConf(Configuration arg0) {
		// TODO Auto-generated method stub
		
	}
}
