import java.io.IOException;
import java.util.Random;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class SimpleRandomSample {
	public static class SimpleRandomSampleMapper extends Mapper <Object, Text, NullWritable, Text>{
		private Random rand = new Random();
		private Double percentage = 0.05;
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			if (rand.nextDouble() < percentage){
				context.write(NullWritable.get(), value);
			}
		}
	}
	
	public static void main (String [] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AverageStdDev");
		job.setJarByClass(SimpleRandomSample.class);
		job.setMapperClass(SimpleRandomSampleMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
}
