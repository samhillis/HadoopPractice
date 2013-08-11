import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;


public class MinMaxCount {

	public static class MinMaxCountMapper extends Mapper <Object, Text, Text, MinMaxCountTuple>{
		private Text carrierID = new Text(); //column 9
		private MinMaxCountTuple outTuple = new MinMaxCountTuple(); //column 15
		
		public void map(Object key, Text value, Context context) throws IOException, InterruptedException{
			String line = value.toString();
	    	String[] fields = line.split(",(?=([^\"]*\"[^\"]*\")*[^\"]*$)");
	    	int delay = 0;
	    	try{
	    		delay = Integer.parseInt(fields[14]);
	    	}
	    	catch(NumberFormatException nfe){
	    		delay = 0;
	    	}
	    	carrierID.set(fields[8]);
	    	outTuple.setCount(1);
	    	outTuple.setMax(delay);
	    	outTuple.setMin(delay);
	    	context.write(carrierID, outTuple);
		}
	}
	
	public static class MinMaxCountReducer extends Reducer <Text, MinMaxCountTuple, Text, MinMaxCountTuple>{
		private MinMaxCountTuple result = new MinMaxCountTuple();
		
		public void reduce (Text key, Iterable<MinMaxCountTuple> values, Context context) throws IOException, InterruptedException{
			result.setMin(0);
			result.setMax(0);
			result.setCount(0);
			int sum = 0;
			
			for (MinMaxCountTuple val : values){
				if (val.getMin() - result.getMin() < 0){
					result.setMin(val.getMin());
				}
				if (val.getMax() - result.getMax() > 0){
					result.setMax(val.getMax());
				}
				sum += val.getCount();
			}
			
		result.setCount(sum);
		context.write(key,result);
		}
	}
	
	public static void main (String [] args) throws Exception{
		Configuration conf = new Configuration();
		Job job = new Job(conf, "MinMaxCount");
		job.setJarByClass(MinMaxCount.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(MinMaxCountTuple.class);
		job.setMapperClass(MinMaxCountMapper.class);
		job.setReducerClass(MinMaxCountReducer.class);
		FileInputFormat.addInputPath(job, new Path(args[0]));
		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		job.waitForCompletion(true);
	}
	
}
