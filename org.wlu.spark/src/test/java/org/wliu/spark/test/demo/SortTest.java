package org.wliu.spark.test.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.wliu.spark.util.CompareCol;
import org.wliu.spark.util.CompareType;
import org.wliu.spark.util.Sort;

import junit.framework.TestCase;
import scala.Tuple2;

public class SortTest extends TestCase {
    public void testRun() throws Exception {
        JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext("local", this.getName());

        ArrayList<List<String>> dataM = new ArrayList<List<String>>();
        dataM.add(Arrays.asList("2", "bb", "elem3"));
        dataM.add(Arrays.asList("1", "zz", "elem1"));
        dataM.add(Arrays.asList("2", "aa", "elem2"));

        JavaRDD<List<String>> rddDataM = ctx.parallelize(dataM);

        // col2(asc)
        CompareCol comp2Asc = new CompareCol(2, true,CompareType.LETTER);

        List<Tuple2<List<String>, List<String>>> output1 = Sort.run(rddDataM, Arrays.asList(comp2Asc)).collect();
        assertEquals(3, output1.size());
        assertEquals(output1.get(0), new Tuple2(Arrays.asList("elem1"), Arrays.asList("1", "zz")));
        assertEquals(output1.get(1), new Tuple2(Arrays.asList("elem2"), Arrays.asList("2", "aa")));
        assertEquals(output1.get(2), new Tuple2(Arrays.asList("elem3"), Arrays.asList("2", "bb")));

        // col0(asc) + col1(asc)
        CompareCol comp0Asc = new CompareCol(0, true,CompareType.LETTER);
        CompareCol comp1Asc = new CompareCol(1, true,CompareType.LETTER);
        List<Tuple2<List<String>, List<String>>> output2 = Sort.run(rddDataM, Arrays.asList(comp0Asc, comp1Asc)).collect();
        assertEquals(3, output2.size());
        assertEquals(output2.get(0), new Tuple2(Arrays.asList("1", "zz"), Arrays.asList("elem1")));
        assertEquals(output2.get(1), new Tuple2(Arrays.asList("2", "aa"), Arrays.asList("elem2")));
        assertEquals(output2.get(2), new Tuple2(Arrays.asList("2", "bb"), Arrays.asList("elem3")));

        // col0(asc) + col1(desc)
        CompareCol comp1Desc = new CompareCol(1, false,CompareType.LETTER);
        List<Tuple2<List<String>, List<String>>> output3 = Sort.run(rddDataM, Arrays.asList(comp0Asc, comp1Desc)).collect();
        assertEquals(3, output3.size());
        assertEquals(output3.get(0), new Tuple2(Arrays.asList("1", "zz"), Arrays.asList("elem1")));
        assertEquals(output3.get(1), new Tuple2(Arrays.asList("2", "bb"), Arrays.asList("elem3")));
        assertEquals(output3.get(2), new Tuple2(Arrays.asList("2", "aa"), Arrays.asList("elem2")));

        // col0(desc) + col1(asc)
        CompareCol comp0Desc = new CompareCol(0, false,CompareType.LETTER);
        List<Tuple2<List<String>, List<String>>> output4 = Sort.run(rddDataM, Arrays.asList(comp0Desc, comp1Asc)).collect();
        assertEquals(3, output4.size());
        assertEquals(output4.get(0), new Tuple2(Arrays.asList("2", "aa"), Arrays.asList("elem2")));
        assertEquals(output4.get(1), new Tuple2(Arrays.asList("2", "bb"), Arrays.asList("elem3")));
        assertEquals(output4.get(2), new Tuple2(Arrays.asList("1", "zz"), Arrays.asList("elem1")));
    }
}