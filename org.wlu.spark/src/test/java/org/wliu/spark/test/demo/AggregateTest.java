package org.wliu.spark.test.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.wliu.spark.util.Aggregate;

import junit.framework.TestCase;
import scala.Tuple2;

public class AggregateTest extends TestCase {
    public void testRun() throws Exception {
        JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext("local", this.getName());

        ArrayList<List<String>> dataM = new ArrayList<List<String>>();
        dataM.add(Arrays.asList("1", "aa", "trtr"));
        dataM.add(Arrays.asList("2", "aa", "tata"));
        dataM.add(Arrays.asList("2", "aa", "titi"));
        dataM.add(Arrays.asList("3", "aa", "toto"));
        dataM.add(Arrays.asList("3", "aa", "toto"));
        dataM.add(Arrays.asList("3", "bb", "tutu"));

        JavaRDD<List<String>> rddDataM = ctx.parallelize(dataM);

        List<Tuple2<List<String>, Iterable<List<String>>>> output = Aggregate.run(rddDataM, Arrays.asList(0, 1)).collect();

        assertEquals(4, output.size());
        
        List<Tuple2<List<String>, Iterable<List<String>>>> outputNull = Aggregate.run(rddDataM, null).collect();

        assertEquals(1, outputNull.size());

        assertEquals(output.get(0), new Tuple2<List<String>, List<List<String>>>(Arrays.asList("1", "aa"), Arrays.asList(Arrays.asList("trtr"))));
        assertEquals(output.get(2), new Tuple2<List<String>, List<List<String>>>(Arrays.asList("2", "aa"), Arrays.asList(Arrays.asList("tata"), Arrays.asList("titi"))));
        assertEquals(output.get(1), new Tuple2<List<String>, List<List<String>>>(Arrays.asList("3", "aa"), Arrays.asList(Arrays.asList("toto"), Arrays.asList("toto"))));
        assertEquals(output.get(3), new Tuple2<List<String>, List<List<String>>>(Arrays.asList("3", "bb"), Arrays.asList(Arrays.asList("tutu"))));
        
        ctx.stop();
        ctx.close();
    }
}
