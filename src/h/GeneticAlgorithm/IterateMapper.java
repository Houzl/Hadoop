package h.GeneticAlgorithm;

import java.io.IOException;

import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.MapReduceBase;
import org.apache.hadoop.mapred.Mapper;
import org.apache.hadoop.mapred.OutputCollector;
import org.apache.hadoop.mapred.Reporter;

public class IterateMapper extends MapReduceBase
    implements
      Mapper<LongWritable, Text, Text, Text> {

  String taskId;
  int keyIndex = 0;
  
  public void configure(JobConf job) {
    taskId = job.get("mapred.task.id");
  }
  
  @Override
  public void map(LongWritable key, Text value, OutputCollector<Text, Text> output,
      Reporter reporter) throws IOException {
    
    String newKey = taskId + "_" + keyIndex/2;
    keyIndex++;
    String newValue = value.toString().split("\t")[1];
    output.collect(new Text(newKey), new Text(newValue));
  }
}
