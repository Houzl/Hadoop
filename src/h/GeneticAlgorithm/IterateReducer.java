package h.GeneticAlgorithm;

import java.io.IOException;
import java.util.Iterator;
import java.util.Random;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reducer;
import org.apache.hadoop.mapred.Reporter;

public class IterateReducer extends MapReduceBase implements Reducer<Text, Text, IntWritable, Text> {
  static Random rand = new Random();
  double crossoverRate = 0.7;
  double mutateRate = 0.001;
  FileSystem fs;
  Path findSign = null;
  double target = 1.0;
  
  public void configure(JobConf job) {
    try {
      findSign = new Path(job.get("chromosomes.findSign"));
      target = Double.parseDouble(job.get("chromosomes.target"));
      mutateRate = Double.parseDouble(job.get("chromosomes.mutateRate"));
      crossoverRate = Double.parseDouble(job.get("chromosomes.crossoverRate"));
      fs = FileSystem.get(job);
      
    } catch (IOException e) {
      e.printStackTrace();
    }
  }

  public void reduce(Text key, Iterator<Text> values, OutputCollector<IntWritable, Text> output,
      Reporter reporter) throws IOException {
    String[] chromosomes = {"",""};
    int i = 0;
    while (values.hasNext()) {
      chromosomes[i] = values.next().toString();
      i++;
    }
    
    if (chromosomes[1].equals("") && (!chromosomes[0].equals(""))){
      //mutate
      chromosomes[0] = ChromosomesStatic.mutate(chromosomes[0], mutateRate, rand);
      double score = ChromosomesStatic.score(chromosomes[0]);
      if (score == target && !fs.exists(findSign)){
        fs.create(findSign);
      }
      output.collect(new IntWritable(rand.nextInt()), new Text(chromosomes[0]));
    }
    else if (!chromosomes[1].equals("") && (!chromosomes[0].equals(""))){
      //crossOver
      chromosomes = ChromosomesStatic.crossOver(chromosomes[0], chromosomes[1], crossoverRate, rand);
      //mutate
      chromosomes[0] = ChromosomesStatic.mutate(chromosomes[0], mutateRate, rand);
      chromosomes[1] = ChromosomesStatic.mutate(chromosomes[1], mutateRate, rand);
      if ((ChromosomesStatic.score(chromosomes[0]) == target ||
          ChromosomesStatic.score(chromosomes[1]) == target) &&
          !fs.exists(findSign)){
        fs.create(findSign);
      }
      output.collect(new IntWritable(rand.nextInt()), new Text(chromosomes[0]));
      output.collect(new IntWritable(rand.nextInt()), new Text(chromosomes[1]));
    }
  }
}
