package org.wliu.spark.test.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.wliu.spark.util.Join;

import junit.framework.TestCase;
import scala.Tuple2;

public class JoinTest extends TestCase {

    public void testJoin() throws Exception {
        JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext("local", this.getName());

        ArrayList<List<String>> dataM1 = new ArrayList<List<String>>();
        dataM1.add(Arrays.asList("1", "trtr"));
        dataM1.add(Arrays.asList("2", "tata"));
        dataM1.add(Arrays.asList("A", "leftJoin"));

        ArrayList<List<String>> dataM2 = new ArrayList<List<String>>();
        dataM2.add(Arrays.asList("1", "titi"));
        dataM2.add(Arrays.asList("2", "toto"));
        dataM2.add(Arrays.asList("B", "cogroupJoin"));

        JavaRDD<List<String>> rddDataM1 = ctx.parallelize(dataM1);
        JavaRDD<List<String>> rddDataM2 = ctx.parallelize(dataM2);

        /* Inner */
        List<Tuple2<List<String>, List<String>>> outputInner = Join.inner(rddDataM1, Arrays.asList(0), rddDataM2, Arrays.asList(0)).collect();

        assertEquals(2, outputInner.size());

        assertTrue(outputInner.contains(new Tuple2<List<String>, List<String>>(Arrays.asList("1"), Arrays.asList("trtr", "titi"))));
        assertTrue(outputInner.contains(new Tuple2<List<String>, List<String>>(Arrays.asList("2"), Arrays.asList("tata", "toto"))));

        /* Left */
        List<Tuple2<List<String>, List<String>>> outputLeft = Join.left(rddDataM1, Arrays.asList(0), rddDataM2, Arrays.asList(0)).collect();

        assertEquals(3, outputLeft.size());

        assertTrue(outputLeft.contains(new Tuple2<List<String>, List<String>>(Arrays.asList("1"), Arrays.asList("trtr", "titi"))));
        assertTrue(outputLeft.contains(new Tuple2<List<String>, List<String>>(Arrays.asList("2"), Arrays.asList("tata", "toto"))));
        assertTrue(outputLeft.contains(new Tuple2<List<String>, List<String>>(Arrays.asList("A"), Arrays.asList("leftJoin"))));

        /* CoGroup */
        List<Tuple2<List<String>, List<String>>> outputCoGroup = Join.cogroup(rddDataM1, Arrays.asList(0), rddDataM2, Arrays.asList(0)).collect();

        assertEquals(3, outputLeft.size());

        assertTrue(outputCoGroup.contains(new Tuple2<List<String>, List<String>>(Arrays.asList("1"), Arrays.asList("trtr", "titi"))));
        assertTrue(outputCoGroup.contains(new Tuple2<List<String>, List<String>>(Arrays.asList("2"), Arrays.asList("tata", "toto"))));
        assertTrue(outputCoGroup.contains(new Tuple2<List<String>, List<String>>(Arrays.asList("A"), Arrays.asList("leftJoin"))));
        assertTrue(outputCoGroup.contains(new Tuple2<List<String>, List<String>>(Arrays.asList("B"), Arrays.asList("cogroupJoin"))));
        
        ctx.stop();
        ctx.close();
    }
}