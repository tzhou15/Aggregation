import java.io.IOException;
import java.util.Iterator;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Reducer;

public class Reduce extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
public void reduce(Text key, Iterable<DoubleWritable> values, Context context) throws IOException, InterruptedException {
	    double min = Integer.MAX_VALUE, max = 0;
	    Iterator<DoubleWritable> iterator = values.iterator();
	    double count = 0;
	    double sum = 0;
	    while (iterator.hasNext()) {
	    	double value = iterator.next().get();
		    sum += iterator.next().get();
		    count++;
		    if(value < min) { 
			    min = value;
		    }
		    if(value > max) { 
			    max = value;
		    }
	    }
	    double average = (int) sum/count;
	    context.write(new Text(key), new DoubleWritable(min));
	    context.write(new Text(key), new DoubleWritable(max));
	    context.write(new Text(key), new DoubleWritable(average));
	    context.write(new Text(key), new DoubleWritable(count));
	   }
}
