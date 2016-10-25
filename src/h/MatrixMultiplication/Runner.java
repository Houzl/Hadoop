/**
 * Learn from https://hadoop.apache.org/docs/r2.7.3/hadoop-mapreduce-client/hadoop-mapreduce-client-core/MapReduceTutorial.html#Mapper
 * @author houzl
 */
package h.MatrixMultiplication;

import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.DoubleWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;

public class Runner {

  public static void main(String[] args) throws Exception {
    //To produce unique intermediate tmp folder
    DateFormat dateFormat = new SimpleDateFormat("yyyyMMddHHmmss");
    Date currentDate = new Date();
    String currentDateString = dateFormat.format(currentDate);
    Path mp = new Path(currentDateString,"pm");
    
    Configuration confFindMP = new Configuration();
    Job jobFindMP = new Job(confFindMP, "Find M and P");
    jobFindMP.setMapperClass(FindMPMapper.class);
    jobFindMP.setReducerClass(FindMPReducer.class);
    jobFindMP.setOutputKeyClass(Text.class);
    jobFindMP.setOutputValueClass(LongWritable.class);
    FileInputFormat.setInputPaths(jobFindMP, new Path("input"));
    FileOutputFormat.setOutputPath(jobFindMP, mp);
    jobFindMP.waitForCompletion(true);
    
    Long m = 0L;
    Long p = 0L;
    //Using hadoop FileSystem get values for m and p
    FileSystem fs = mp.getFileSystem(confFindMP);
    FSDataInputStream inputStream = fs.open(new Path(mp.toString() + "/part-r-00000"));
    //readLine method is deprecated, should find another method.
    String[] line = inputStream.readLine().split("\t");
    if (line[0].equals("M")) m = Long.parseLong(line[1]);
    if (line[0].equals("P")) p = Long.parseLong(line[1]);
    line = inputStream.readLine().split("\t");
    if (line[0].equals("M")) m = Long.parseLong(line[1]);
    if (line[0].equals("P")) p = Long.parseLong(line[1]);
    inputStream.close();
    
    Configuration confMultiple = new Configuration();
    Path tmpoutput = new Path(currentDateString,"MultipleCell");
    confMultiple.setLong("m", m);
    confMultiple.setLong("p", p);
    Job jobMultiple = new Job(confMultiple, "Multiple elements");
    jobMultiple.setMapperClass(MultiplyMapper.class);
    jobMultiple.setReducerClass(MultiplyReducer.class);
    jobMultiple.setOutputKeyClass(Text.class);
    jobMultiple.setOutputValueClass(DoubleWritable.class);
    FileInputFormat.setInputPaths(jobMultiple, new Path("input"));
    FileOutputFormat.setOutputPath(jobMultiple, tmpoutput);
    jobMultiple.waitForCompletion(true);
    
    Configuration confPlus = new Configuration();
    Job jobPlus = new Job(confPlus, "plus cell");
    jobPlus.setMapperClass(PlusMapper.class);
    jobPlus.setReducerClass(PlusReducer.class); 
    jobPlus.setMapOutputValueClass(DoubleWritable.class);
    jobPlus.setOutputKeyClass(Text.class); 
    jobPlus.setOutputValueClass(NullWritable.class); 
    FileInputFormat.setInputPaths(jobPlus, tmpoutput);
    FileOutputFormat.setOutputPath(jobPlus, new Path("output"));
    jobPlus.waitForCompletion(true);
  }

}
