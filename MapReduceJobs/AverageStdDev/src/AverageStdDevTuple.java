import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import org.apache.hadoop.io.Writable;

public class AverageStdDevTuple implements Writable {

	private float ave;
	private float stdev;

	public float getAve(){
		return ave;
	}
	
	public void setAve(float ave){
		this.ave = ave;
	}

	public float getStdDev(){
		return stdev;
	}
	
	public void setStdDev(float stdev){
		this.stdev= stdev;
	}
	
	public void readFields(DataInput in) throws IOException{
		ave = in.readFloat();
		stdev = in.readFloat();
	}

	public void write(DataOutput out) throws IOException{
		out.writeFloat(ave);
		out.writeFloat(stdev);
	}
	
	public String toString() {
		return ave + "\t" + stdev;
	}
}
