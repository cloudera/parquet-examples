/**
 * Licensed to Cloudera, Inc. under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  Cloudera, Inc. licenses this file
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
package com.cloudera.parquet.hadoop;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.schema.GroupType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example MR job implementation which reads data from a parquet file and
 * writes the same data to csv.
 */
public class TestReadParquet extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(TestReadParquet.class);

  /*
   * Read a Parquet record, write a CSV record
   */
  public static class ReadRequestMap extends Mapper<LongWritable, Group, NullWritable, Text> {

    @Override
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
      NullWritable outKey = NullWritable.get();
      StringBuilder csv = new StringBuilder();

      GroupType schema = value.getType();
      boolean hasContent = false;
      // Iterate over every field in the Parquet record
      for (int i = 0, n = schema.getFieldCount(); i < n; ++i) {

        // Skipping complex types
        if (!schema.getType(i).isPrimitive()) {
          LOG.info("Skipping complex type: {}", value.getValueToString(i, 0));
          continue;
        }

        if (hasContent) {
          csv.append(',');
        }
        // Do not add anything to the csv if the field value is missing (NULL)
        if (value.getFieldRepetitionCount(i) > 0) {
          String entry = value.getValueToString(i, 0);
          boolean mustQuote = (entry.contains(",") || entry.contains("'"));
          if (mustQuote) {
            csv.append('"');
          }
          csv.append(entry);
          if (mustQuote) {
            csv.append('"');
          }
        }
        hasContent = true;
      }

      context.write(outKey, new Text(csv.toString()));
    }
  }

  public int run(String[] args) throws Exception {
    getConf().set("mapreduce.output.textoutputformat.separator", ",");

    Job job = Job.getInstance(getConf());
    job.setJarByClass(getClass());
    job.setJobName(getClass().getName());

    job.setMapOutputKeyClass(LongWritable.class);
    job.setMapOutputValueClass(Text.class);
    job.setOutputKeyClass(Text.class);
    job.setOutputValueClass(Text.class);

    job.setMapperClass(ReadRequestMap.class);
    job.setNumReduceTasks(0);

    job.setInputFormatClass(ExampleInputFormat.class);
    job.setOutputFormatClass(TextOutputFormat.class);

    FileInputFormat.setInputPaths(job, new Path(args[0]));
    FileOutputFormat.setOutputPath(job, new Path(args[1]));

    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    try {
      int res = ToolRunner.run(new Configuration(), new TestReadParquet(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(255);
    }
  }
}
