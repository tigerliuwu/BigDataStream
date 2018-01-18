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

package org.apache.spark.examples;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Iterator;
import java.util.List;
import java.util.regex.Pattern;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.FlatMapFunction;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;

public final class JavaWordCount {
	private static final Pattern SPACE = Pattern.compile(" ");

	public static void main(String[] args) throws Exception {

		/*
		 * if (args.length < 1) { System.err.println(
		 * "Usage: JavaWordCount <file>"); System.exit(1); }
		 */

		SparkConf sparkConf = new SparkConf().setAppName("JavaWordCount");
		sparkConf.setMaster("local[*]");
		JavaSparkContext ctx = new JavaSparkContext(sparkConf);
		JavaRDD<String> lines = ctx.textFile("testdata/wordcount/", 2);

		JavaRDD<String> words = lines.flatMap(new FlatMapFunction<String, String>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = -5362343744041430068L;

			@Override
			public Iterable<String> call(String s) {
				return Arrays.asList(SPACE.split(s));
			}
		});
		words = words.filter(new Function<String, Boolean>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -5945744427187855692L;

			public Boolean call(String v1) throws Exception {
				// TODO Auto-generated method stub
				return !"dont".equalsIgnoreCase(v1);
			}

		});

		words = words.map(new Function<String, String>() {

			public String call(String v1) throws Exception {
				String v2 = v1;
				if (v2.equalsIgnoreCase("world")) {
					v2 = v2.toUpperCase();
				}
				return v2;
			}

		});

		words = words.mapPartitions(new FlatMapFunction<Iterator<String>, String>() {

			public Iterable<String> call(Iterator<String> t) throws Exception {
				List<String> results = new ArrayList<String>();
				while (t.hasNext()) {
					results.add(t.next() + "_append");
				}

				return results;
			}

		});

		JavaPairRDD<String, Integer> ones = words.mapToPair(new PairFunction<String, String, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 2120732597737862849L;

			public Tuple2<String, Integer> call(String s) {
				return new Tuple2<>(s, 1);
			}
		});

		JavaPairRDD<String, Integer> counts = ones.reduceByKey(new Function2<Integer, Integer, Integer>() {
			/**
			 * 
			 */
			private static final long serialVersionUID = 5383594096515786199L;

			public Integer call(Integer i1, Integer i2) {
				return i1 + i2;
			}
		});

		counts.saveAsNewAPIHadoopDataset(ctx.hadoopConfiguration());

		List<Tuple2<String, Integer>> output = counts.collect();
		for (Tuple2<?, ?> tuple : output) {
			System.out.println(tuple._1() + ": " + tuple._2());
		}
		ctx.stop();
		ctx.close();
	}
}
