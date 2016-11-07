package h.GeneticAlgorithm;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.logging.Level;
import java.util.logging.Logger;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.FileInputFormat;
import org.apache.hadoop.mapred.FileOutputFormat;
import org.apache.hadoop.mapred.JobClient;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextOutputFormat;
import org.apache.hadoop.mapred.lib.NLineInputFormat;

public class Runner {

	/**
	 * @param args
	 *            [0] input file directory args[1] output file directory --
	 *            existing directory will be deleted
	 */
  public static void main(String[] args) throws Exception {
    try {
      double target = Double.parseDouble(args[0]);
      int chromosomesLength = Integer.parseInt(args[1]);
      double crossoverRate = Double.parseDouble(args[2]);
      double mutateRate = Double.parseDouble(args[3]);
      int maxIteration = Integer.parseInt(args[4]);
      int numMapper = Integer.parseInt(args[5]);
      int numReducer = Integer.parseInt(args[6]);
      int countPerMapper = Integer.parseInt(args[7]);
      String outputPath = args[8];

      runJob(target, chromosomesLength, crossoverRate, mutateRate, maxIteration, numMapper,
          numReducer, countPerMapper, outputPath);
      
    } catch (IOException ex) {
      Logger.getLogger(Runner.class.getName()).log(Level.SEVERE, null, ex);
    }
  }

  public static void runJob(double target, int chromosomesLength, double crossoverRate, double mutateRate,
      int maxIteration, int numMapper, int numReducer, int countPerMapper, String outputPath)
      throws IOException {
  
    DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    Date currentDate = new Date();
    String currentDateString = dateFormat.format(currentDate);
    
    JobClient client = new JobClient();
    JobConf poolBuilderConf = new JobConf(Runner.class);
    poolBuilderConf.setJobName("GA Pool Builder");
    // specify a map function
    poolBuilderConf.setMapperClass(PoolBuilderMapper.class);
    // Using IdentityReducer, Performs no reduction, writing all input values directly to the
    // output.
    poolBuilderConf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);
    poolBuilderConf.setInputFormat(SleepInputFormat.class);
    poolBuilderConf.setInt("mapreduce.job.maps", numMapper);
    poolBuilderConf.setInt("mapreduce.job.emit.count", countPerMapper);
    poolBuilderConf.setInt("chromosomes.length", chromosomesLength);
    poolBuilderConf.set("chromosomes.findSign", currentDateString + "/findSignPath");
    poolBuilderConf.set("chromosomes.target", Double.toString(target));

    
    poolBuilderConf.setOutputKeyClass(IntWritable.class);
    poolBuilderConf.setOutputValueClass(Text.class);
    
    Path poolBuilderPath = new Path( currentDateString + "/poolBuilderPath");
    Path findSignPath = new Path( currentDateString + "/findSignPath");

    
    // FileInputFormat.addInputPath(jobConf, new Path(input));
    FileOutputFormat.setOutputPath(poolBuilderConf, poolBuilderPath);


    client.setConf(poolBuilderConf);
    try {
      JobClient.runJob(poolBuilderConf);
    } catch (IOException ex) {
      Logger.getLogger(Runner.class.getName()).log(Level.SEVERE, null, ex);
    }
    
    Path poolIterateOutputPath = poolBuilderPath;
    Path poolIterateInputPath;
    Path findSign = new Path(currentDateString + "/findSignPath");
    
    FileSystem dfs = FileSystem.get(findSign.toUri(), poolBuilderConf);
    int i = 0;
    while (!dfs.exists(findSign) && i < maxIteration){
      poolIterateInputPath = poolIterateOutputPath;
      poolIterateOutputPath = new Path( currentDateString + "/poolIterateOutputPath-" + i); 
      client = new JobClient();
      JobConf poolIterateConf = new JobConf(Runner.class);
      poolIterateConf.setJobName("GA Iteration");
      // specify a map function
      poolIterateConf.setMapperClass(IterateMapper.class);
      // Using IdentityReducer, Performs no reduction, writing all input values directly to the
      // output.
      poolIterateConf.setReducerClass(IterateReducer.class);
      
      poolIterateConf.setInputFormat(NLineInputFormat.class);
      poolIterateConf.setOutputFormat(TextOutputFormat.class);
      FileInputFormat.addInputPath(poolIterateConf, poolIterateInputPath);
      FileOutputFormat.setOutputPath(poolIterateConf, poolIterateOutputPath);
      poolIterateConf.setInt("mapred.line.input.format.linespermap", countPerMapper);
      poolIterateConf.set("chromosomes.findSign", currentDateString + "/findSignPath");
      poolIterateConf.set("chromosomes.target", Double.toString(target));
      poolIterateConf.set("chromosomes.mutateRate", Double.toString(mutateRate));
      poolIterateConf.set("chromosomes.crossoverRate", Double.toString(crossoverRate));
      poolIterateConf.setMapOutputKeyClass(Text.class);
      poolIterateConf.setOutputKeyClass(IntWritable.class);
      poolIterateConf.setOutputValueClass(Text.class);
      poolIterateConf.setNumReduceTasks(numReducer);
      
      client.setConf(poolIterateConf);
      try {
        JobClient.runJob(poolIterateConf);
      } catch (IOException ex) {
        Logger.getLogger(Runner.class.getName()).log(Level.SEVERE, null, ex);
      }
      
      client = new JobClient();
      JobConf poolIterateMergeConf = new JobConf(Runner.class);
      poolIterateMergeConf.setJobName("GA iterate merge Builder");
      // specify a map function
      poolIterateMergeConf.setMapperClass(IterateMergeMapper.class);
      // Using IdentityReducer, Performs no reduction, writing all input values directly to the
      // output.
      poolIterateMergeConf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);
      poolIterateMergeConf.setInputFormat(NLineInputFormat.class);
      poolIterateMergeConf.setInt("mapred.line.input.format.linespermap", countPerMapper);
      
      poolIterateMergeConf.setOutputKeyClass(IntWritable.class);
      poolIterateMergeConf.setOutputValueClass(Text.class);
      
      poolIterateInputPath = poolIterateOutputPath;
      poolIterateOutputPath = new Path( currentDateString + "/poolIterateOutputPath-" + i +"-merge"); 
      
      FileInputFormat.addInputPath(poolIterateMergeConf, poolIterateInputPath);
      FileOutputFormat.setOutputPath(poolIterateMergeConf, poolIterateOutputPath);
      
      client.setConf(poolIterateMergeConf);
      try {
        JobClient.runJob(poolIterateMergeConf);
      } catch (IOException ex) {
        Logger.getLogger(Runner.class.getName()).log(Level.SEVERE, null, ex);
      }
      
      dfs = FileSystem.get(findSign.toUri(), poolIterateConf);
      i++;
    }
    
    client = new JobClient();
    JobConf viewerConf = new JobConf(Runner.class);
    viewerConf.setJobName("GA viewer Builder");
    // specify a map function
    viewerConf.setMapperClass(viewerMapper.class);
    // Using IdentityReducer, Performs no reduction, writing all input values directly to the
    // output.
    viewerConf.setReducerClass(org.apache.hadoop.mapred.lib.IdentityReducer.class);
    viewerConf.setInputFormat(NLineInputFormat.class);
    viewerConf.setInt("mapred.line.input.format.linespermap", countPerMapper);
    viewerConf.setInt("mapreduce.job.maps", numMapper);
    viewerConf.set("chromosomes.findSign", currentDateString + "/findSignPath");
    viewerConf.set("chromosomes.target", Double.toString(target));
    
    viewerConf.setOutputKeyClass(FloatWritable.class);
    viewerConf.setOutputValueClass(Text.class);
    
    Path outPath = new Path(outputPath);
    
    FileInputFormat.addInputPath(viewerConf, poolIterateOutputPath);
    FileOutputFormat.setOutputPath(viewerConf, outPath);
   
    dfs = FileSystem.get(outPath.toUri(), viewerConf);
    if (dfs.exists(outPath)) {
      dfs.delete(outPath, true);
    }
    
    client.setConf(viewerConf);
    try {
      JobClient.runJob(viewerConf);
    } catch (IOException ex) {
      Logger.getLogger(Runner.class.getName()).log(Level.SEVERE, null, ex);
    }

    
  }

}
