/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.examples.adaptivecheckpointing;

import org.apache.flink.api.common.JobExecutionResult;
import org.apache.flink.api.common.functions.MapFunction;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.CheckpointingMode;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.windowing.time.Time;

/**
 * Check Functionality for Adaptive Checkpoint.
 */
public class AdaptiveCheckpoint {
	static StreamExecutionEnvironment env;

	public static void main(String[] args) throws Exception {
		//isRunning = true;
		env = StreamExecutionEnvironment.getExecutionEnvironment();
        // base/start interval in milliseconds
		env.enableCheckpointing(3000);
        // set mode to exactly-once (this is the default)
		env.getCheckpointConfig().setCheckpointingMode(CheckpointingMode.EXACTLY_ONCE);
        // checkpoints have to complete within one minute, or are discarded
		env.getCheckpointConfig().setCheckpointTimeout(600000);
        // allow only one checkpoint to be in progress at the same time
		env.getCheckpointConfig().setMaxConcurrentCheckpoints(1);

		DataStream<Tuple2<String, Integer>> dataStream = env.addSource(new DataGenerator())
			.map(new MyMapper())
			.keyBy(0)
			.timeWindow(Time.milliseconds(200))
			.sum(1);

		JobExecutionResult result = env.execute("Adaptive CheckPoint");
	}

	/**
	 * Mapper Class.
	 */
	public static class MyMapper implements MapFunction<String, Tuple2<String, Integer>> {

		public Tuple2<String, Integer> map(String s) throws Exception {

			String[] spilitedData = s.split(",");

			Tuple2<String, Integer> tuple2 = new Tuple2<String, Integer>();

			tuple2.setField(spilitedData[0], 0);
			tuple2.setField(Integer.parseInt(spilitedData[1]), 1);

			return tuple2;
		}
	}
}
