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

import static org.apache.parquet.format.converter.ParquetMetadataConverter.SKIP_ROW_GROUPS;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.LocatedFileStatus;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.RemoteIterator;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.hadoop.ParquetFileReader;
import org.apache.parquet.hadoop.example.ExampleInputFormat;
import org.apache.parquet.hadoop.example.ExampleOutputFormat;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.apache.parquet.schema.MessageType;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * An example MR job implementation which reads data from a parquet file and
 * writes the same data in parquet with the specified compression codec.
 */
public class TestReadWriteParquet extends Configured implements Tool {
  private static final Logger LOG = LoggerFactory.getLogger(TestReadWriteParquet.class);

  /*
   * Read a Parquet record, write a Parquet record
   */
  public static class ReadRequestMap extends Mapper<LongWritable, Group, Void, Group> {
    @Override
    public void map(LongWritable key, Group value, Context context) throws IOException, InterruptedException {
      // We might filter/modify records here by creating a new Group and setting the required elements.
      // For example:
      // MessageType schema = MessageTypeParser.parseMessageType("message schema { required int32 x; required int64 y; }");
      // GroupFactory factory = new SimpleGroupFactory(schema);
      // Group group = factory.newGroup();
      // group.append("x", value.getInteger("other_x", 0));
      // group.append("y", value.getLong("other_y", 0));
      // context.write(null, value);

      // For now, simply write the original value
      context.write(null, value);
    }
  }

  public int run(String[] args) throws Exception {
    if (args.length < 2) {
      LOG.error("Usage: {} INPUTFILE OUTPUTFILE [compression]", getClass().getName());
      return 1;
    }
    String inputFile = args[0];
    String outputFile = args[1];
    String compression = (args.length > 2) ? args[2] : "none";

    Path parquetFilePath = null;
    // Find a file in case a directory was passed
    RemoteIterator<LocatedFileStatus> it = FileSystem.get(getConf()).listFiles(new Path(inputFile), true);
    while (it.hasNext()) {
      FileStatus fs = it.next();
      if (fs.isFile()) {
        parquetFilePath = fs.getPath();
        break;
      }
    }
    if (parquetFilePath == null) {
      LOG.error("No file found for {}", inputFile);
      return 1;
    }
    LOG.info("Getting schema from {}", parquetFilePath);
    ParquetMetadata footer = ParquetFileReader.readFooter(getConf(), parquetFilePath, SKIP_ROW_GROUPS);
    MessageType schema = footer.getFileMetaData().getSchema();
    LOG.info("Retrieved schema: {}", schema);
    GroupWriteSupport.setSchema(schema, getConf());

    Job job = Job.getInstance(getConf());
    job.setJarByClass(getClass());
    job.setJobName(getClass().getName());
    job.setMapperClass(ReadRequestMap.class);
    job.setNumReduceTasks(0);
    job.setInputFormatClass(ExampleInputFormat.class);
    job.setOutputFormatClass(ExampleOutputFormat.class);

    CompressionCodecName codec = CompressionCodecName.UNCOMPRESSED;
    if (compression.equalsIgnoreCase("snappy")) {
      codec = CompressionCodecName.SNAPPY;
    } else if (compression.equalsIgnoreCase("gzip")) {
      codec = CompressionCodecName.GZIP;
    }
    LOG.info("Output compression: {}", codec);
    ExampleOutputFormat.setCompression(job, codec);

    FileInputFormat.setInputPaths(job, new Path(inputFile));
    FileOutputFormat.setOutputPath(job, new Path(outputFile));

    job.waitForCompletion(true);

    return 0;
  }

  public static void main(String[] args) throws Exception {
    try {
      int res = ToolRunner.run(new Configuration(), new TestReadWriteParquet(), args);
      System.exit(res);
    } catch (Exception e) {
      e.printStackTrace();
      System.exit(255);
    }
  }
}
