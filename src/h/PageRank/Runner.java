/**
 * @author houzl
 */
package h.PageRank;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Runner {

  public static void main(String[] args) throws Exception {
    if (args.length != 4){
      System.err.println("Please input 4 arguments: input output numberofpages numberofiterations");
      return;
    }
    
    Long n = 0l;
    Long ii = 1l;
    try{
      n = Long.parseLong(args[2]);
      ii = Long.parseLong(args[3]);
    }
    catch (Exception e){
      System.err.println("The third and forth arguments must be number");
      return;
    }
    Path input = new Path(args[0]);
    Path output = new Path(args[1]);
    for(long i = 1; i <= ii; i++){
      Configuration iteration = new Configuration();
      iteration.setLong("n", n);
      if (i > 1l) input = output;
      output = new Path("Iteration/Iter-" + i);
      Job jobIteration = new Job(iteration, "PageRank Iteration" + i);
      jobIteration.setMapperClass(IterationMapper.class);
      jobIteration.setReducerClass(IterationReducer.class); 
      jobIteration.setMapOutputValueClass(Text.class);
      jobIteration.setOutputKeyClass(Text.class); 
      jobIteration.setOutputValueClass(NullWritable.class); 
      FileInputFormat.setInputPaths(jobIteration, input);
      FileOutputFormat.setOutputPath(jobIteration, output);
      jobIteration.waitForCompletion(true);
    }
    
    //Viewer Job.
    Configuration viewer = new Configuration();
    Job jobViewer = new Job(viewer, "PageRank Viewer");
    jobViewer.setMapperClass(ViewerMapper.class);
    jobViewer.setOutputKeyClass(DoubleWritable.class);
    FileInputFormat.setInputPaths(jobViewer, output);
    FileOutputFormat.setOutputPath(jobViewer, new Path(args[1]));
    jobViewer.waitForCompletion(true);
  }

}
