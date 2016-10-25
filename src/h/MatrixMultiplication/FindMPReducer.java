/**
 * @author houzl
 * Learn from https://hadoop.apache.org/docs/r2.7.3/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Mapper
 */
package h.MatrixMultiplication;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class FindMPReducer extends Reducer<Text, LongWritable, Text, LongWritable> {
  
  private Long maxM = 0L;
  private Long maxP = 0L;

  public void reduce(Text key, Iterable<LongWritable> values, Context context)
      throws IOException, InterruptedException {
    
    //Key String
    String k = key.toString();
    for (LongWritable val : values) {
      //Long value
      Long v = val.get();
      if (k.equals("M") && v > maxM)
        maxM = v;
      else if (k.equals("P") && v > maxP) maxP = v;
    }
    if (k.equals("M")) context.write(key, new LongWritable(maxM));
    if (k.equals("P")) context.write(key, new LongWritable(maxP));
  }
}
