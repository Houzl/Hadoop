/**
 * Learn from https://hadoop.apache.org/docs/r2.7.3/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Mapper
 * @author houzl
 */
package h.MatrixMultiplication;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class MultiplyReducer extends Reducer<Text, DoubleWritable, Text, DoubleWritable> {
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {

    String[] keyStrArray = key.toString().split("_");
    //New key for output, m_p
    String newKey = keyStrArray[0] + "_" + keyStrArray[2];
    
    int valueCount = 0;
    double total = 1;
    for(DoubleWritable val : values){
      valueCount++;
      total *= val.get();
    }
    //If only has one element, this means there is zero element which we don't care.
    if (valueCount == 2){
      context.write(new Text(newKey), new DoubleWritable(total));
    }
  }
}
