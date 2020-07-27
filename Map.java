import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object, Text, Text, DoubleWritable> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		String time = tokens[0].replaceAll("\\:|\\.|\\-|", "");
		String hourData = time.substring(8,9);
		String minData = time.substring(10,11);
		String secData = time.substring(12,13);
		int hour = Integer.parseInt(hourData);
		int min = Integer.parseInt(minData);
		int sec = Integer.parseInt(secData);
		int id = Integer.parseInt(tokens[1]);
		String Key = hour + "" + "" + min + "" + sec + "" + id;
		int i;
		for(i=2; i<tokens.length; i++) {
			context.write(new Text(Key + "" + String.valueOf("var00"+(i+1))), new DoubleWritable(Double.parseDouble(tokens[i])));
		}
	}
}
