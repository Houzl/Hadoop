/**
 * Learn from https://hadoop.apache.org/docs/r2.7.3/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Mapper
 * @author houzl
 */
package h.MatrixMultiplication;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class PlusMapper extends Mapper<Object, Text, Text, DoubleWritable> {

  private Text newKey = new Text();
  private DoubleWritable newValue = new DoubleWritable();

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {

    String[] strValueArray = value.toString().split("\t");
    newKey.set(strValueArray[0]);
    newValue.set(Double.parseDouble(strValueArray[1]));
    context.write(newKey, newValue);
  }
}

