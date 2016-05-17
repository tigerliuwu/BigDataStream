package org.wliu.spark.test.demo;

import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.wliu.spark.util.CompareCol;
import org.wliu.spark.util.CompareType;
import org.wliu.spark.util.KeyBy;
import org.wliu.spark.util.KeyByCompareCol;

import junit.framework.TestCase;
import scala.Tuple2;

public class KeyByTest extends TestCase {

    public void testRun() throws Exception {
        JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext("local", this.getName());

        List<String> e1 = Arrays.asList("1", "1", "elem1");
        List<String> e2 = Arrays.asList("2", "2", "elem2");
        List<String> e3 = Arrays.asList("3", "3", "elem3");
        List<List<String>> dataM = Arrays.asList(e1, e2, e3);

        JavaRDD<List<String>> rddDataM = ctx.parallelize(dataM);

        List<Tuple2<List<String>, List<String>>> outputKeyBy = KeyBy.run(rddDataM, Arrays.asList(0, 1)).collect();
        
        assertEquals(3, outputKeyBy.size());
        assertTrue(outputKeyBy.contains(new Tuple2(Arrays.asList("1", "1"), Arrays.asList("elem1"))));
        assertTrue(outputKeyBy.contains(new Tuple2(Arrays.asList("2", "2"), Arrays.asList("elem2"))));
        assertTrue(outputKeyBy.contains(new Tuple2(Arrays.asList("3", "3"), Arrays.asList("elem3"))));


        CompareCol compCol0 = new CompareCol(0, true,CompareType.LETTER);
        CompareCol compCol1 = new CompareCol(1, true,CompareType.LETTER);
        List<Tuple2<List<String>, List<String>>> outputKeyByComp = KeyByCompareCol.run(rddDataM, Arrays.asList(compCol0, compCol1)).collect();


        assertEquals(3, outputKeyByComp.size());
        assertTrue(outputKeyByComp.contains(new Tuple2(Arrays.asList("1", "1"), Arrays.asList("elem1"))));
        assertTrue(outputKeyByComp.contains(new Tuple2(Arrays.asList("2", "2"), Arrays.asList("elem2"))));
        assertTrue(outputKeyByComp.contains(new Tuple2(Arrays.asList("3", "3"), Arrays.asList("elem3"))));
    }
}