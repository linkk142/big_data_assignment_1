package hadoop_ece.assignment_1.practice_2;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;


public class Practice_2 {
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private IntWritable friends = new IntWritable();
		private Text ageLvl = new Text();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(";");

			try{
				int age = Integer.parseInt(itr[2]);
				if (age>=0 && age<=5){
					ageLvl.set("0-5 years");
				}
				else if (age>=6 && age<=12){
					ageLvl.set("6-12 years");
				}
				else if (age>=13 && age<=17){
					ageLvl.set("13-17 years");
				}
				else if (age>=18 && age<=25){
					ageLvl.set("18-25 years");
				}
				else if (age>=26 && age<=35){
					ageLvl.set("26-35 years");
				}
				else if (age>=36 && age<=45){
					ageLvl.set("36-45 years");
				}
				else if (age>=46 && age<=60){
					ageLvl.set("46-60 years");
				}
				else if (age>=60){
					ageLvl.set("60+ years");
				}
				friends.set(Integer.parseInt(itr[4]));
				context.write(ageLvl,friends);
			}catch(NumberFormatException e){
				// Cas de la première ligne
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,FloatWritable> {
		private FloatWritable result = new FloatWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			float avg = 0;
			
			for (IntWritable val : values) {
				sum += val.get();
				avg++;
			}
			avg = sum/avg;
			result.set(avg);
			context.write(key, result);
		}
	}
	
	public Practice_2(String[] args) throws Exception {
		Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // Create Conf
		Configuration conf = new Configuration(true);
		conf.set("mapred.textoutputformat.separator", ";");
		
		// Create Job
		Job job = new Job(conf, "Practice 2");
		job.setJarByClass(Practice_2.class);
		
		// Set Map Reduce
		job.setMapperClass(TokenizerMapper.class);
		//job.setCombinerClass(IntSumReducer.class);
		job.setReducerClass(IntSumReducer.class);
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
		
		System.exit(job.waitForCompletion(true) ? 0 : 1);
	}
}
