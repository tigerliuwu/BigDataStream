package org.wliu.spark.test.demo;

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.wliu.spark.util.Load;
import org.wliu.spark.util.Store;

import junit.framework.TestCase;

/**
 * Company : Altic - LIPN
 * User: Tugdual Sarazin
 * Date: 18/04/14
 * Time: 17:02
 */
public class LoadTest extends TestCase {
    public void testRun() throws Exception {
        JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext("local", this.getName());

        String pathName = "/tmp/testOutputLoad";
        String separator = ";";

        ArrayList dataM = new ArrayList();
        dataM.add(Arrays.asList("1", "elem1"));
        dataM.add(Arrays.asList("2", "elem2"));
        dataM.add(Arrays.asList("3", "elem3"));

        JavaRDD rddDataM = ctx.parallelize(dataM);

        // Store JavaRDD
        Store.run(pathName, rddDataM, separator);

        // Test
        List<List<String>> outputRDD = Load.run(ctx, pathName, separator).collect();
        assertEquals(3, outputRDD.size());
        assertTrue(outputRDD.contains(Arrays.asList("1", "elem1")));
        assertTrue(outputRDD.contains(Arrays.asList("2", "elem2")));
        assertTrue(outputRDD.contains(Arrays.asList("3", "elem3")));

        // Cleaning
        StoreTest.delete(new File(pathName));
    }
}
