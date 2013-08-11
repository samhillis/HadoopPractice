import java.io.IOException;
import java.util.ArrayList;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class AverageStdDev {

	public static class AverageStdDevMapper extends Mapper <Object, Text, Text, IntWritable>{
		private Text carrierID = new Text(); //column 9
		private IntWritable delay = new IntWritable();
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
	    	String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	    	delay.set(0);
	    	try{
	    		delay.set(Integer.parseInt(fields[14]));
	    	}
	    	catch(NumberFormatException nfe){
	    		delay.set(0);
	    	}
	    	carrierID.set(fields[8]);
	    	context.write(carrierID, delay);
		}
	}
	
	public static class AverageStdDevReducer extends Reducer <Text, IntWritable, Text, AverageStdDevTuple>{
		private AverageStdDevTuple result = new AverageStdDevTuple();
		private ArrayList<Float> delayTime = new ArrayList<Float>();
		
		public void reduce (Text key, Iterable<IntWritable> values, Context context) throws IOException, InterruptedException{
			delayTime.clear();
			result.setAve(0.0f);
			result.setStdDev(0.0f);
			float sum = 0.0f;
			float count = 0.0f;
			
			for (IntWritable val : values){
				delayTime.add((float) val.get());
				count += 1.0f;
				sum += val.get();
			}
			
			float mean = sum/count;
			result.setAve(mean);
			float sumOfSquares = 0.0f;
			for (Float f : delayTime){
				sumOfSquares += (f - mean)*(f - mean);
			}
			
			result.setStdDev((float) Math.sqrt(sumOfSquares/(count - 1)));
			context.write(key,result);
		}
	}
	
	public static void main (String [] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "AverageStdDev");
		job.setJarByClass(AverageStdDev.class);
		job.setMapperClass(AverageStdDevMapper.class);
		job.setReducerClass(AverageStdDevReducer.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(AverageStdDevTuple.class);
		/*necessary because mapper and reducer output differing value classes*/
		job.setMapOutputValueClass(IntWritable.class);
		
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
	
}
