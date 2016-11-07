package h.GeneticAlgorithm;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class PoolBuilderMapper extends MapReduceBase
    implements
      Mapper<IntWritable, IntWritable, IntWritable, Text> {


  
  static Random rand = new Random();
  private int length = 1;
  FileSystem fs;
  Path findSign = null;
  double target = 0;
  
  public void configure(JobConf job) {
    try {
      length = job.getInt("chromosomes.length", 9);
      findSign = new Path(job.get("chromosomes.findSign"));
      target = Double.parseDouble(job.get("chromosomes.target"));
      fs = FileSystem.get(job);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void map(IntWritable key, IntWritable value, OutputCollector<IntWritable, Text> output,
      Reporter reporter) throws IOException {
    int count = key.get();
    for (int i = 0; i < count; ++i) {
      int newKey = rand.nextInt();
      String chromosomes = ChromosomesStatic.randomGenerate(length, rand);
      double score = ChromosomesStatic.score(chromosomes);
      if (score == target && !fs.exists(findSign)){
        fs.create(findSign);
      }
      String newValue = chromosomes;
      output.collect(new IntWritable(newKey), new Text(newValue));
    }
  }
}
