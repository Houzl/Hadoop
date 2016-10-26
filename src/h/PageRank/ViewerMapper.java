/**
 * @author houzl
 */
package h.PageRank;

import java.io.IOException;

import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class ViewerMapper extends Mapper<Object, Text, DoubleWritable, Text> {

  private DoubleWritable newKey = new DoubleWritable();
  private Text newValue = new Text();

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    // The input format is Page + " " + PageRank(Page) + " " + Outlink1 + " " + Outlink2 + " " +
    // Outlink3
    String[] strValueArray = value.toString().split(" ", 3);

    if (strValueArray.length == 3) {
      newKey.set(Double.parseDouble(strValueArray[1].trim()) * -1);
      newValue.set(strValueArray[0].trim());
      context.write(newKey, newValue);
    }
  }
}

