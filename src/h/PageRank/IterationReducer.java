/**
 * @author houzl
 */
package h.PageRank;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Reducer;

public class IterationReducer extends Reducer<Text, Text, NullWritable, Text> {
  
  //Set damping factor
  private final static double d = 0.85;
  
  public void reduce(Text key, Iterable<Text> values, Context context)
      throws IOException, InterruptedException {
    
    //Get the total number of pages. 
    Configuration conf = context.getConfiguration();
    long N = conf.getLong("n", 100000l);
    
    double pageRankTotal = 0;
    String outlinks = "";
    for(Text val : values){
      String v = val.toString();
      if (v.startsWith("##OL##")){
        outlinks = v.substring(6).trim();
      }
      else if(v.startsWith("##PR##")){
        pageRankTotal += Double.parseDouble(v.substring(6));
      }
    }
    
    //Use the formula to get the new PageRank
    Double newPageRank = (1-d)/N + d * pageRankTotal;
    String newValueString = key.toString().trim() + " " + newPageRank + " " + outlinks;
    context.write(NullWritable.get(),new Text(newValueString));
  }
}


