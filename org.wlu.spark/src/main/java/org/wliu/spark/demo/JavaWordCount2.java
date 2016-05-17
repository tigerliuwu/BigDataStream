package org.wliu.spark.demo;

import java.util.ArrayList;
import java.util.List;

import org.apache.spark.SparkConf;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function2;
import org.apache.spark.api.java.function.PairFlatMapFunction;

import scala.Tuple2;

public class JavaWordCount2 {
	
	public static void main(String[] args) {
		SparkConf sparkConf = new SparkConf().setAppName("word Count");
		sparkConf.setMaster("local");
		JavaSparkContext sparkContext = new JavaSparkContext(sparkConf);
		JavaRDD<String> file = sparkContext.textFile("/home/liuwu/Desktop/temp/test1.txt", 1);
		// step 1: flatten the input to one partition
		JavaPairRDD<String, Integer> words = file.flatMapToPair(new PairFlatMapFunction<String,String,Integer>(){

			/**
			 * 
			 */
			private static final long serialVersionUID = -63965425614344500L;

			@Override
			public Iterable<Tuple2<String, Integer>> call(String t) throws Exception {
				String[] arr = t.split(" ");
				List<Tuple2<String, Integer>> results = new ArrayList<Tuple2<String, Integer>>();
				for (String str : arr) {
					results.add(new Tuple2<String, Integer>(str,1));
				}
				return results;
			}
			
		});
		
		JavaPairRDD<String, Integer> result = words.reduceByKey(new Function2<Integer, Integer, Integer>() {

			/**
			 * 
			 */
			private static final long serialVersionUID = -8829422808101234772L;

			@Override
			public Integer call(Integer v1, Integer v2) throws Exception {
				// TODO Auto-generated method stub
				return v1+v2;
			}
			
		});
		
		
		List<Tuple2<String, Integer>> outs = result.collect();
		
		for (Tuple2<String, Integer> t : outs) {
			System.out.println(t._1.toString() + "\t" + t._2);
		}
		
		
		sparkContext.stop();
		sparkContext.close();
	}

}
