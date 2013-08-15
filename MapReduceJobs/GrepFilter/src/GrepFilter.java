import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class GrepFilter {
	
	public static class GrepFilterMapper extends Mapper <Object, Text, NullWritable, Text>{
		private String myRegex = "AQ";
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
	    	String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	    	String carrierID = fields[8];
			if (carrierID.matches(myRegex)){
				context.write(NullWritable.get(), value);
			}
		}
	}
	
	public static void main (String [] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AverageStdDev");
		job.setJarByClass(GrepFilter.class);
		job.setMapperClass(GrepFilterMapper.class);
		job.setOutputKeyClass(NullWritable.class);
		job.setOutputValueClass(Text.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
	
}
