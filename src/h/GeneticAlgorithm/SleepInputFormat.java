/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package h.GeneticAlgorithm;

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;

import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

/**
 * Dummy class for testing MR framefork. Sleeps for a defined period of time in mapper and reducer.
 * Generates fake input for map / reduce jobs. Note that generated number of input pairs is in the
 * order of <code>numMappers * mapSleepTime / 100</code>, so the job uses some disk space.
 */
public class SleepInputFormat extends Configured
    implements
      InputFormat<IntWritable, IntWritable> {
  public InputSplit[] getSplits(JobConf conf, int numSplits) {
    numSplits = conf.getInt("mapreduce.job.maps", 1);
    numSplits = numSplits < 0 ? 1 : numSplits;
    InputSplit[] ret = new InputSplit[numSplits];
    for (int i = 0; i < numSplits; ++i) {
      ret[i] = new EmptySplit();
    }
    return ret;
  }

  public RecordReader<IntWritable, IntWritable> getRecordReader(InputSplit ignored, JobConf conf,
      Reporter reporter) throws IOException {
    final int count = 1;
    final int emitCount = conf.getInt("mapreduce.job.emit.count", 1);
    if (emitCount < 0) throw new IOException("Invalid out count: " + emitCount);
    return new RecordReader<IntWritable, IntWritable>() {
      private int records = 0;

      public boolean next(IntWritable key, IntWritable value) throws IOException {
        key.set(emitCount);
        value.set(0);
        return records++ < count;
      }

      public IntWritable createKey() {
        return new IntWritable();
      }

      public IntWritable createValue() {
        return new IntWritable();
      }

      public long getPos() throws IOException {
        return records;
      }

      public void close() throws IOException {}

      public float getProgress() throws IOException {
        return records / ((float) count);
      }
    };
  }
  
  public static class EmptySplit implements InputSplit {
    public void write(DataOutput out) throws IOException { }
    public void readFields(DataInput in) throws IOException { }
    public long getLength() { return 0L; }
    public String[] getLocations() { return new String[0]; }
  }
}
