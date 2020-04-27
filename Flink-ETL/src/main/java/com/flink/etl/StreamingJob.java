/*
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

package com.flink.etl;

import org.apache.flink.api.common.serialization.SimpleStringEncoder;
import org.apache.flink.api.common.serialization.SimpleStringSchema;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsStateBackend;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.CheckpointConfig;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.sink.filesystem.StreamingFileSink;
import org.apache.flink.streaming.api.functions.sink.filesystem.rollingpolicies.DefaultRollingPolicy;
import org.apache.flink.streaming.connectors.kafka.FlinkKafkaConsumer010;

import java.util.Properties;
import java.util.concurrent.TimeUnit;

/**
 * Skeleton for a Flink Streaming Job.
 *
 * <p>
 * For a tutorial how to write a Flink streaming application, check the
 * tutorials and examples on the
 * <a href="http://flink.apache.org/docs/stable/">Flink Website</a>.
 *
 * <p>
 * To package your application into a JAR file for execution, run 'mvn clean
 * package' on the command line.
 *
 * <p>
 * If you change the name of the main class (with the public static void
 * main(String[] args)) method, change the respective entry in the POM.xml file
 * (simply search for 'mainClass').
 */
public class StreamingJob {

	public static void main(String[] args) throws Exception {
		// set up the streaming execution environment
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();

		Properties props = new Properties();
		props.setProperty("bootstrap.servers", "192.168.3.122:9092");
		props.setProperty("group.id", "test");
		FlinkKafkaConsumer010<String> consumer = new FlinkKafkaConsumer010<>("data-collection-topic",
				new SimpleStringSchema(), props);
		DataStream<String> stream = env.addSource(consumer);

		DefaultRollingPolicy rollingPolicy = DefaultRollingPolicy.create().withMaxPartSize(1024 * 1024 * 128) // 设置每个文件的最大大小
																												// ,默认是128M。这里设置为128M
				.withRolloverInterval(TimeUnit.MINUTES.toMillis(86400)) // 滚动写入新文件的时间，默认60s。这里设置为无限大
				.withInactivityInterval(TimeUnit.MINUTES.toMillis(60)) // 60s空闲，就滚动写入新的文件
				.build();

		StreamingFileSink<String> sink = StreamingFileSink
//				.forRowFormat(new Path("file:///tmp/kafka-loader"), new SimpleStringEncoder<String>())
				.forRowFormat(new Path("hdfs://DATASEA/home/dmp_operator1/bpointdata"),
						new SimpleStringEncoder<String>())
				.withBucketAssigner(new EventTimeBucketAssigner()).withRollingPolicy(rollingPolicy).build();
		stream.addSink(sink);

		env.enableCheckpointing(10_000);
		env.setParallelism(1);
		env.setStateBackend((StateBackend) new FsStateBackend("hdfs://DATASEA/home/dmp_operator1/flink/checkpoints"));
		env.getCheckpointConfig()
		.enableExternalizedCheckpoints(CheckpointConfig.ExternalizedCheckpointCleanup.DELETE_ON_CANCELLATION);

		// execute program
		env.execute("Flink Streaming Java API Skeleton");
	}
}
