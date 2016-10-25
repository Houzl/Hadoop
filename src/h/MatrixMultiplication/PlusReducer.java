/**
 * Learn from https://hadoop.apache.org/docs/r2.7.3/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Mapper
 * @author houzl
 */
package h.MatrixMultiplication;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class PlusReducer extends Reducer<Text, DoubleWritable, Text, NullWritable> {
  public void reduce(Text key, Iterable<DoubleWritable> values, Context context)
      throws IOException, InterruptedException {
 
    double total = 1;
    for(DoubleWritable val : values){
      //Sum elements.
      total += val.get();
    }
    context.write(new Text(key.toString().replace('_', ',') + "," + total), NullWritable.get());
  }
}


