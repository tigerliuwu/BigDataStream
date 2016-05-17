package org.wliu.spark.util;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

public class Aggregate {
    public static JavaPairRDD<List<String>, Iterable<List<String>>> run(final JavaRDD<List<String>> dataM, final List<Integer> colsId) throws Exception {
        JavaPairRDD<List<String>, List<String>> pairDataM = KeyBy.run(dataM, colsId);
        return pairDataM.groupByKey();
    }
    
}