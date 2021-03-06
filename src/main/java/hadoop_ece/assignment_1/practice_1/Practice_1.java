package hadoop_ece.assignment_1.practice_1;
import java.io.IOException;
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

public class Practice_1 {
	
	public static class TokenizerMapper extends Mapper<Object, Text, Text, IntWritable>{
		private Text location = new Text();
		private IntWritable balanceAccount = new IntWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
			String[] itr = value.toString().split(";");
			location.set(itr[1]);
			try{
				balanceAccount.set(Integer.parseInt(itr[3]));
				context.write(location,balanceAccount);
			}catch(NumberFormatException e){
				// Cas de la première ligne
			}
		}
	}
	
	public static class IntSumReducer extends Reducer<Text,IntWritable,Text,IntWritable> {
		private IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values,Context context) throws IOException, InterruptedException {
			int sum = 0;
			
			for (IntWritable val : values) {
				sum += val.get();
			}
			
			result.set(sum);
			context.write(key, result);
		}
	}
	
	public Practice_1(String[] args) throws Exception {
		
		Path inputPath = new Path(args[0]);
        Path outputDir = new Path(args[1]);

        // Create Conf
		Configuration conf = new Configuration(true);
		conf.set("mapred.textoutputformat.separator", ";");
		// Create Job
		Job job = new Job(conf, "Practice 1");
		job.setJarByClass(Practice_1.class);
		
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
