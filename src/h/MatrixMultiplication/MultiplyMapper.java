/**
 * Learn from https://hadoop.apache.org/docs/r2.7.3/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Mapper
 * @author houzl
 */
package h.MatrixMultiplication;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class MultiplyMapper extends Mapper<Object, Text, Text, DoubleWritable> {

  private Text newKey = new Text();
  private DoubleWritable newValue = new DoubleWritable();

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    
    //Get m and p from runner.
    Configuration conf = context.getConfiguration();
    long m = conf.getLong("m", 1l);
    long p = conf.getLong("p", 1l);

    String[] strValueArray = value.toString().split(",");
    if (strValueArray[0].equals("A")) {
      //The elements of A will be used p+1 time.
      for (long i = 0; i <= p; i++) {
        //Key will be m_n_p
        newKey.set(strValueArray[1] + "_" + strValueArray[2] + "_" + i);
        newValue.set(Long.parseLong(strValueArray[3]));
        context.write(newKey, newValue);
      }
    } 
    else if (strValueArray[0].equals("B")) {
      //The elements of B will be used m+1 time.
      for (long i = 0; i <= m; i++) {
        //Key will be m_n_p
        newKey.set(i + "_" + strValueArray[1] + "_" + strValueArray[2]);
        newValue.set(Long.parseLong(strValueArray[3]));
        context.write(newKey, newValue);
      }
    }
  }
}

