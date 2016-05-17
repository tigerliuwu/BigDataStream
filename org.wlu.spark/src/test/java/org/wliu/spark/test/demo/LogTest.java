package org.wliu.spark.test.demo;

import java.util.ArrayList;
import java.util.Arrays;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.wliu.spark.util.KeyBy;
import org.wliu.spark.util.Log;

import junit.framework.TestCase;

public class LogTest extends TestCase {

    public void testRun() throws Exception {
        JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext("local", this.getName());

        ArrayList dataM = new ArrayList();
        dataM.add(Arrays.asList("1", "elem1", "aaa"));
        dataM.add(Arrays.asList("2", "elem2", "bbb"));
        dataM.add(Arrays.asList("3", "elem3", "ccc"));

        JavaRDD rddDataM = ctx.parallelize(dataM);

        Log.run(rddDataM);

        Log.run(KeyBy.run(rddDataM, Arrays.asList(0)));
    }
}