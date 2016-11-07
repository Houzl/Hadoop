package h.GeneticAlgorithm;

import java.io.IOException;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class viewerMapper extends MapReduceBase
    implements
      Mapper<LongWritable, Text, FloatWritable, Text> {


  
  static Random rand = new Random();
  FileSystem fs;
  Path findSign = null;
  double target = 0;
  
  public void configure(JobConf job) {
    try {
      findSign = new Path(job.get("chromosomes.findSign"));
      target = Double.parseDouble(job.get("chromosomes.target"));
      fs = FileSystem.get(job);
    } catch (IOException e) {
      e.printStackTrace();
    }
  }
  
  @Override
  public void map(LongWritable key, Text value, OutputCollector<FloatWritable, Text> output,
      Reporter reporter) throws IOException {
    
    String chromosomes = value.toString().split("\t")[1];
    double score = ChromosomesStatic.score(chromosomes);
    double newKey = Math.abs(target - score);
    String newValue = chromosomes + ", " + ChromosomesStatic.decode(chromosomes) + ", " + score;
    output.collect(new FloatWritable((float) newKey), new Text(newValue));
  }
}
