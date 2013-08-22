import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class movieReviewCount {
	
	public static class Map extends Mapper <Object, Text, Text, IntWritable>{
		private final static IntWritable count = new IntWritable(1);
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
			String[] fields = line.split("\t");
			context.write(new Text(fields[0]), count);
		}
		
	}
	
	public static class Red extends Reducer <Text, IntWritable, Text, IntWritable>{
		private final static IntWritable result = new IntWritable();
		
		public void reduce(Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			int count = 0;
			for (IntWritable val : values){
				count += val.get();
			}
			result.set(count);
			context.write(key, result);
		}
		
	}
	
	public static void main (String [] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "movieReviewCount");
		job.setJarByClass(movieReviewCount.class);
		job.setMapperClass(Map.class);
		job.setReducerClass(Red.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(IntWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(IntWritable.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}