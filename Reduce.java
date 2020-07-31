import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
	public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
		double min = Integer.MAX_VALUE; 
		double max = 0.0;
		double sum = 0;
		double count = 0;
		Iterator<DoubleWritable> iterator = values.iterator(); 
		while (iterator.hasNext()) {
		    double value = iterator.next().get();
			if (value < min) {      
				min = value;
			}
			if (value > max) { 
				max = value;
			}
			sum = sum + value;
			count ++;
		}
		double average =  sum / count;
		context.write(new Text(key), new DoubleWritable(min));
		context.write(new Text(key), new DoubleWritable(max));
		context.write(new Text(key), new DoubleWritable(average));
	}
}
