package hadoop_ece.assignment_1;

import java.io.IOException;
import java.util.Scanner;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import hadoop_ece.assignment_1.practice_1.Practice_1;
import hadoop_ece.assignment_1.practice_2.Practice_2;

public class Main 
{
	public static class WordCountReducer extends Reducer<Text, IntWritable, Text, IntWritable> {

		public void reduce(Text text, Iterable<IntWritable> values, Context context)
		        throws IOException, InterruptedException {
		    int sum = 0;
		    for (IntWritable value : values) {
		        sum += value.get();
		    }
		    context.write(text, new IntWritable(sum));
		}
	}
	
	public static class WordCountMapper extends Mapper<Object, Text, Text, IntWritable> {

		private final IntWritable ONE = new IntWritable(1);
		private Text word = new Text();
		
		public void map(Object key, Text value, Context context)
		        throws IOException, InterruptedException {
		
		    String[] csv = value.toString().split(",");
		    for (String str : csv) {
		        word.set(str);
		        context.write(word, ONE);
		    }
		}
	}
	
    public static void main( String[] args ) throws Exception
    {
    	try{
    		System.out.println("Input file : " + args[0]);
    		System.out.println("Output file : " + args[1]);
    	}catch(Exception e){
    		System.out.println("No input and output file specified");
    		System.exit(0);
    	}
    	System.out.println("Press 1 to execute the first practice.\nPress 2 to execute the second practice.\nPress 3 to execute the Word Count example");
    	Scanner sc = new Scanner(System.in);
    	int choice = sc.nextInt();
    	switch(choice){
    		case 1:
    			// Execute practice 1
    			new Practice_1(args);
    			break;
	    	case 2:
	    		// Execute practice 2
	    		new Practice_2(args);
	    		break;
	    	case 3:
	    		Path inputPath = new Path(args[0]);
	            Path outputDir = new Path(args[1]);
	     
	            // Create configuration
	            Configuration conf = new Configuration(true);
	     
	            // Create job
	            Job job = new Job(conf, "WordCount");
	            job.setJarByClass(WordCountMapper.class);
	     
	            // Setup MapReduce
	            job.setMapperClass(WordCountMapper.class);
	            job.setReducerClass(WordCountReducer.class);
	            job.setNumReduceTasks(1);
	     
	            // Specify key / value
	            job.setOutputKeyClass(Text.class);
	            job.setOutputValueClass(IntWritable.class);
	     
	            // Input
	            FileInputFormat.addInputPath(job, inputPath);
	            job.setInputFormatClass(TextInputFormat.class);
	     
	            // Output
	            FileOutputFormat.setOutputPath(job, outputDir);
	            job.setOutputFormatClass(TextOutputFormat.class);
	     
	            // Delete output if exists
	            FileSystem hdfs = FileSystem.get(conf);
	            if (hdfs.exists(outputDir))
	                hdfs.delete(outputDir, true);
	     
	            // Execute job
	            int code = job.waitForCompletion(true) ? 0 : 1;
	            System.exit(code);
	    		break;
    		default :
    			break;
    	}
    	sc.close();
    }
}
