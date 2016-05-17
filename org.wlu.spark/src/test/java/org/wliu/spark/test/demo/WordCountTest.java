package org.wliu.spark.test.demo;

import java.util.List;

import org.apache.spark.api.java.JavaSparkContext;

import junit.framework.TestCase;
import scala.Tuple2;


public class WordCountTest extends TestCase {
    public void testRun() throws Exception {
        JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext("local", this.getName());

        WordCount wc = new WordCount();
        List<Tuple2<String, Integer>> output = wc.run(ctx, "ressources/toto.data");

        assertEquals(3, output.size());

        assertTrue(output.contains(new Tuple2("toto", 2)));
        assertTrue(output.contains(new Tuple2("tutu", 1)));
        assertTrue(output.contains(new Tuple2("titi", 1)));
    }
}
