import java.io.IOException;
import org.apache.hadoop.io.*;
import org.apache.hadoop.mapreduce.*;


public class Map extends Mapper<LongWritable, Text, Text, DoubleWritable> {
	public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		String time = tokens[0].replaceAll("\\:|\\.|\\-|", "");
		try {
			if (key.get() == 0 && value.toString().contains("header")) {
				return;
			} else {
				for (int i = 2; i < tokens.length; i++) { 
					context.write(new Text(time.substring(0,6)+"\t"+tokens[1]+"\t"+String.valueOf(i-1)),new DoubleWritable(Double.parseDouble(tokens[i])));
				}
			} 
		} catch (Exception e) {
			e.printStackTrace();
		} 
	}
}
