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

import org.apache.flink.streaming.api.functions.source.SourceFunction;

import java.util.Random;

/**
 * Data Generator for Adaptive Checkpoint.
 */
public class DataGenerator implements SourceFunction<String> {

    //static int MAX_Number_Of_Tuples = 90000;
	static final int MAX_NUMBER_OF_TUPLES = 4294967;
    //numberofTuples = 4294967295, Size = 34359.73 MB -> 34 GB
	Random random = new Random();
	static int tupleNumber = MAX_NUMBER_OF_TUPLES;

	public void run(SourceContext<String> sourceContext) throws Exception {

		while (tupleNumber > 0) {
			//String s =
			sourceContext.collect("W" + tupleNumber + ",1");
			tupleNumber--;
			Thread.sleep(1);
		}
	}

	public void cancel() {

	}

	public String getTuple(int n) {

		switch (n) {
			case 0:
				return "A";
			case 1:
				return "AB";
			case 2:
				return "ABC";
			case 3:
				return "ABCD";
			case 4:
				return "ABCDE";
			case 5:
				return "ABCDEF";
			case 6:
				return "ABCDEFG";
			case 7:
				return "ABCDEFGH";
			case 8:
				return "ABCDEFGHI";
			case 9:
				return "ABCDEFGHIJ";
			default:
				return "ABCDEFGHIJKLMNOPQRSTUVWXYZ";
		}
	}
}
