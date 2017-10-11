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
package org.apache.ben;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.conf.Configured;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.util.Tool;
import org.apache.hadoop.util.ToolRunner;

/**
 * This class runs a Mapreduce program on the input folder removing all new line characters between
 * quotes.
 * New line characters in database text fields is a common data preparation problem 
 * and this mapreduce program uses a custom Record Reader to remove any new line characters between 
 * quotes"
 * I.e. Text like 
 * 1,"Ben","Ben was working
 * on our cluster"
 * 2,"Karl","Karl is an hadoop engineer"
 * Would be translated to
 * 1,"Ben","Ben was working on our cluster"
 * 2,"Karl","Karl is an hadoop engineer"
 * To use a different quote character than " set the QUOTE value in the QuotationLineReader
 * private static final byte QUOTE = '\"';
 * To change the way newline characters are handled change the processing in the mapper
 * line = line.replaceAll("\n", "\\n");
 * line = line.replaceAll("\r", "\\r");
 * @author bleonhardi
 *
 */
public class FileCleaningFactory extends Configured implements Tool
{

	public static void main(String[] args) throws Exception
	{
		int exit = ToolRunner.run(new Configuration(), new FileCleaningFactory(), args);
		System.exit(exit);
	}

	@Override
	public int run(String[] args) throws Exception
	{
		Configuration conf = new Configuration();
		// setting the input split size 64lMB or 128MB are good.
	
		Job job = new Job(conf, "FileCleaningMapreduce");
		// now we optionally set the delimiter and replacement character
		
		job.setJarByClass(getClass());
		job.setJobName("FileCleaningRead");

		job.setNumReduceTasks(0);
		// Finally we have to set our input and output format classes
		job.setInputFormatClass(FileCleaningInputFormat.class);
		//job.setOutputFormatClass(TikaOutputFormat.class);

		FileInputFormat.addInputPath(job, new Path(args[0]));
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(Text.class);
		job.setMapperClass(FileCleaningMapper.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		FileOutputFormat.setOutputPath(job, new Path(args[1]));
		return job.waitForCompletion(true) ? 0 : 1;
	}
}
