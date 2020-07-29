import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
    public void map(LongWritable key, Text value, Context context) throws IOException {
    	String[] tokens = value.toString().split(",");
		String Key = tokens[1].trim();
		String Value = tokens[2];
    	try {
            if (key.get() == 0 && value.toString().contains("header")) {
                return;
        	} else {
        		int i;
        		for(i=3; i<tokens.length; i++) {
        			Value = Value + "\t" + tokens[i];
        		} 
        	}
        } catch (Exception e) {
            e.printStackTrace();
         }
    	context.write(new Text(Key), new DoubleWritable(Double.parseDouble(Value))); 
        }
}
