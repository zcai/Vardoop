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

//package org.apache.hadoop.mapreduce.lib.input;
//package org.apache.hadoop.streaming;

import java.io.BufferedInputStream;
import java.io.DataInputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.ByteOrder;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.io.compress.CompressionCodec;
import org.apache.hadoop.io.compress.CompressionCodecFactory;
import org.apache.hadoop.mapreduce.InputSplit;
import org.apache.hadoop.mapreduce.RecordReader;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.apache.hadoop.util.LineReader;
import org.apache.commons.logging.LogFactory;
import org.apache.commons.logging.Log;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.zip.Inflater;
/**
 * Treats keys as offset in file and value as line. 
 */
public class BamRecordReader extends RecordReader<Text, Text> {
	private static final Log LOG = LogFactory.getLog(BamRecordReader.class);
	public static byte [] head = null;//make the array large enough to fit the bam head
	private CompressionCodecFactory compressionCodecs = null;
	private long start;
	private long pos;
	private long split_length;
	private int maxLineLength;
	private Text key = null;
	private Text value = null;
	private Path file = null;
	private FSDataInputStream fileIn = null;
	Path fileInfo = null;
	private int flag = 0;
	public void initialize(InputSplit genericSplit,TaskAttemptContext context) throws IOException {
		FileSplit split = (FileSplit) genericSplit;
		Configuration job = context.getConfiguration();
		this.maxLineLength = job.getInt("mapred.linerecordreader.maxlength",Integer.MAX_VALUE);
		start = split.getStart();
		split_length = split.getLength();
		System.out.println("start: "+start);
		System.out.println("split_length: "+split_length);
		fileInfo = split.getPath();
	}

	public boolean nextKeyValue() throws IOException {//must return false when no input data available
		if (key == null) {
			key = new Text();
		}
		key.set(fileInfo.toString());
		if (value == null) {
			value = new Text();
		}
		value.set((new Long(start)).toString()+"#"+(new Long(split_length)).toString());
		if (flag == 0){
			flag = 1;
			return true;
		}else{
			//when no data available
			return false;
		}
	}

	@Override
	public Text getCurrentKey() {
		return key;
	}

	@Override
	public Text getCurrentValue() {
		return value;
	}

	/**
	* Get the progress within the split
	*/
	public float getProgress() {
/*
		if (start == end) {
			return 0.0f;
		} else {
			return Math.min(1.0f, (pos - start) / (float)(end - start));
		}
*/
		if (flag == 0){
			return 0.0f;
		}else{
			return 1.0f;
		}
	}
	public synchronized void close() throws IOException {
		if (in != null) {
			in.close();
		}
	}

}


