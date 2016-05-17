package org.wliu.spark.util;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;
import org.apache.spark.api.java.function.Function2;
import scala.Tuple2;

import java.util.Iterator;
import java.util.List;

/**
 * Company : Altic - LIPN
 * User: Tugdual Sarazin
 * Date: 05/05/14
 * Time: 15:42
 */
public class Store {
    public static void run(String pathName, final JavaRDD<List<String>> dataM, final String separator) throws Exception {
        JavaRDD<String> rddDataTxt = dataM.map(new Function<List<String>, String>() {
            public String call(List<String> row) {
                StringBuilder line = new StringBuilder();
                for (int i=0; i<row.size(); i++) {
                    // Append word
                    line.append(row.get(i));
                    // Append separator
                    if (i < row.size() - 1) {
                        line.append(separator);
                    }
                }
                return line.toString();
            }
        });
        rddDataTxt.saveAsTextFile(pathName);
    }

    public static void run(String pathName, final JavaPairRDD<List<String>, List<String>> dataM, final String separator) throws Exception {
        JavaRDD<String> rddDataTxt = dataM.map(new Function<Tuple2<List<String>, List<String>>, String>() {
            public String call(Tuple2<List<String>, List<String>> row) {
                StringBuilder line = new StringBuilder();
                // add keys to the row string
                for (Iterator<String> it = row._1().iterator(); it.hasNext();) {
                    line.append(it.next());
                    if (it.hasNext()) {
                        line.append(separator);
                    }
                }
                // add values to the row string
                for (Iterator<String> it = row._2().iterator(); it.hasNext();) {
                    line.append(separator);
                    line.append(it.next());
                }
                return line.toString();
            }
        });
        rddDataTxt.saveAsTextFile(pathName);
    }
}