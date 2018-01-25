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
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.GroupFactory;
import org.apache.parquet.example.data.simple.SimpleGroupFactory;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.MessageTypeParser;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example MR job implementation which reads data from a csv file and writes
 * the first 2 columns to a parquet file.
 */
public class TestWriteParquet extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(TestWriteParquet.class);

  /*
   * Read a csv record, write a Parquet record
   */
  public static class ReadRequestMap extends Mapper<LongWritable, Text, Void, Group> {
    private GroupFactory factory;

    @Override
    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
      String[] record = value.toString().split(",");

      if (factory == null) {
        factory = new SimpleGroupFactory(GroupWriteSupport.getSchema(context.getConfiguration()));
      }

      Group group = factory.newGroup();
      group.append("x", record[0]);
      group.append("y", record[1]);

      context.write(null, group);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      LOG.error("Usage: {} INPUTFILE OUTPUTFILE", getClass().getName());
      return 1;
    }
    String inputFile = args[0];
    String outputFile = args[1];

    Path csvFilePath = null;
    // Find a file in case a directory was passed
    RemoteIterator<LocatedFileStatus> it = FileSystem.get(getConf()).listFiles(new Path(inputFile), true);
    while (it.hasNext()) {
      FileStatus fs = it.next();
      if (fs.isFile()) {
        csvFilePath = fs.getPath();
        break;
      }
    }
    if (csvFilePath == null) {
      LOG.error("No file found for {}", inputFile);
      return 1;
    }

    Job job = Job.getInstance(getConf());
    job.setJarByClass(getClass());
    job.setJobName(getClass().getName());
    job.setMapperClass(ReadRequestMap.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(TextInputFormat.class);
    job.setOutputFormatClass(ExampleOutputFormat.class);

    MessageType schema = MessageTypeParser
        .parseMessageType("message example {"
            + "required binary x (UTF8);"
            + "required binary y (UTF8);"
            + "}");
    LOG.info("Setting output parquet schema: {}", schema);
    ExampleOutputFormat.setSchema(job, schema);

    CompressionCodecName codec = CompressionCodecName.SNAPPY;
    LOG.info("Output compression: {}", codec);
    ExampleOutputFormat.setCompression(job, codec);

    FileInputFormat.setInputPaths(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));

    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    try {
      int res = ToolRunner.run(new Configuration(), new TestWriteParquet(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(255);
    }
  }
}
