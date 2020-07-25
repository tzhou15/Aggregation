import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Partitioner;

public class Partition extends Partitioner<Text, Text> {
	public int getPartition(Text key, Text value, int numReduceTasks) {
		Map m = new Map();
		if(numReduceTasks==0)
			return 0;
		if(hour<=24) {
			int a;
			for(a=1; a<=33; a++) {
				if(tokens[1]=a) {
					return a % numReduceTasks;
				}else{
					continue;
				}
			}
		}else if(hour>24 && hour<=48) {
			int b;
			for(b=1; b<=33; b++) {
				if(tokens[1]=b) {
					b=b+33;
					return b % numReduceTasks;
				}else{
					continue;
				}
			}
		}else if(hour>48) {
			int c;
			for(c=1; c<=33; c++) {
				if(tokens[1]=c) {
					c=c+66;
					return c % numReduceTasks;
				}else{
					continue;
				}
			}
		}
}
}
