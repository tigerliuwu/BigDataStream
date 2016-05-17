package org.wliu.spark.test.demo;

import java.io.File;
import java.io.IOException;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.wliu.spark.util.KeyBy;
import org.wliu.spark.util.Load;
import org.wliu.spark.util.Store;

import junit.framework.TestCase;

public class StoreTest extends TestCase {

    public void testRun() throws Exception {
        JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext("local", this.getName());

        String pathName = "/tmp/testOutputStore";
        String separator = ";";

        List<String> e1 = Arrays.asList("1", "1", "elem1");
        List<String> e2 = Arrays.asList("2", "2", "elem2");
        List<String> e3 = Arrays.asList("3", "3", "elem3");
        List<List<String>> dataM = Arrays.asList(e1, e2, e3);

        JavaRDD rddDataM = ctx.parallelize(dataM);

        // Store JavaRDD
        Store.run(pathName, rddDataM, separator);

        // Test
        List<List<String>> outputRDD = Load.run(ctx, pathName, separator).collect();
        assertEquals(3, outputRDD.size());
        assertTrue(outputRDD.contains(e1));
        assertTrue(outputRDD.contains(e2));
        assertTrue(outputRDD.contains(e3));

        // Cleaning
        delete(new File(pathName));

        // Store JavaPairRDD
        Store.run(pathName, KeyBy.run(rddDataM, Arrays.asList(0, 1)), separator);

        // Test
        List<List<String>> outputPairRDD = Load.run(ctx, pathName, separator).collect();
        assertEquals(3, outputPairRDD.size());
        assertTrue(outputPairRDD.contains(e1));
        assertTrue(outputPairRDD.contains(e2));
        assertTrue(outputPairRDD.contains(e3));

        // Cleaning
        delete(new File(pathName));
    }

    public static void delete(File file) throws IOException {
        if(file.isDirectory()) {
            //directory is empty, then delete it
            if(file.list().length==0){
                file.delete();
            }else{
                //list all the directory contents
                String files[] = file.list();

                for (String temp : files) {
                    //construct the file structure
                    File fileDelete = new File(file, temp);
                    //recursive delete
                    delete(fileDelete);
                }

                //check the directory again, if empty then delete it
                if(file.list().length==0){
                    file.delete();
                }
            }
        }else{
            //if file, then delete it
            file.delete();
        }
    }
}