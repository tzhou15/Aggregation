import java.io.IOException;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class Map extends Mapper<Object, Text, Text, Text> {
	public void map(Object key, Text value, Context context) throws IOException, InterruptedException {
		String[] tokens = value.toString().split(",");
		String time = tokens[0].replaceAll("\\:|\\.|", "");
		String hourData = time.substring(0, 2);
		int hour = Integer.parseInt(hourData);
		String Key = time.trim() + "\t" + tokens[1].trim();
		String Value = tokens[2];
		int i;
		for(i=3; i<68; i++) {
			Value = Value + "\t" + tokens[i];
		}
		context.write(new Text(Key), new Text(Value));
	}
}