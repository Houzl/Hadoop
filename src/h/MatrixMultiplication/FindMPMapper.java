/**
 * Learn from https://hadoop.apache.org/docs/r2.7.3/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Mapper
 * @author houzl
 */
package h.MatrixMultiplication;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class FindMPMapper extends Mapper<Object, Text, Text, LongWritable> {

  private Text newKey = new Text();
  private LongWritable newValue = new LongWritable();

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    
    String[] strValueArray = value.toString().split(",");
    
    if (strValueArray[0].equals("A")) {
      newKey.set("M");
      newValue.set(Long.parseLong(strValueArray[1].trim()));
      context.write(newKey, newValue);
    } 
    else if (strValueArray[0].equals("B")) {
      newKey.set("P");
      newValue.set(Long.parseLong(strValueArray[2].trim()));
      context.write(newKey, newValue);
    }
  }
}
