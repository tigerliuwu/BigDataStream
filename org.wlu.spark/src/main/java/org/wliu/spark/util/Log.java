package org.wliu.spark.util;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;
import scala.util.parsing.combinator.testing.Str;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * Company : Altic - LIPN
 * User: Tugdual Sarazin
 * Date: 05/05/14
 * Time: 15:23
 */
public class Log {
    public static void run(final JavaRDD<List<String>> dataM) throws Exception {
        for (List<String> row : dataM.collect()) {
            System.out.println(row);
        }
    }

    public static void run(final JavaPairRDD<List<String>, List<String>> dataM) throws Exception {
        for (Tuple2<List<String>, List<String>> row : dataM.collect()) {
            System.out.println(row);
        }
    }
}
