/**
 * @author houzl
 */
package h.PageRank;

import java.io.IOException;

import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;

public class IterationMapper extends Mapper<Object, Text, Text, Text> {

  private Text newKey = new Text();
  private Text newValue = new Text();

  public void map(Object key, Text value, Context context)
      throws IOException, InterruptedException {
    // The input format is Page + " " + PageRank(Page) + " " + Outlink1 + " " + Outlink2 + " " +
    // Outlink3
    String[] strValueArray = value.toString().split(" ", 3);

    if (strValueArray.length == 3) {
      // Emit outlinks. outlinks for each page never change.
      newKey.set(strValueArray[0].trim());
      // Use "##OL##" as flag to tell the reducer this is outlinks.
      newValue.set("##OL##" + strValueArray[2].trim());
      context.write(newKey, newValue);
      
      // If current page didn't vote to any other page. Its PageRank will lost in next iteration.
      if (strValueArray[2].trim() == null || strValueArray[2].trim().equals("")) return;
      
      String[] outlinks = strValueArray[2].trim().split(" ");
      long numOutlinks = outlinks.length;

      // If current page vote to other pages, pass value to the target page.
      Double outPR = Double.parseDouble(strValueArray[1].trim()) / numOutlinks;
      for (String outlink : outlinks) {
        // Key will the page, who is voted by current page.
        
        newKey.set(outlink.trim());
        // Value will the PR(page)/L(page), who is voted by current page.
        // Use "##PR##" as flag to tell the reducer this is PR(page)/L(page).
        newValue.set("##PR##" + outPR.toString());
        context.write(newKey, newValue);
      }
    }
  }
}

