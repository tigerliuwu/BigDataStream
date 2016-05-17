package org.wliu.spark.util;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;

/**
 * Company : Altic - LIPN
 * User: Tugdual Sarazin
 * Date: 22/04/14
 * Time: 12:48
 */

public class KeyByCompareCol {
    public static JavaPairRDD<List<String>, List<String>> run(final JavaRDD<List<String>> dataM, final List<CompareCol> compCols) throws Exception {
        return dataM.mapToPair(new PairFunction<List<String>, List<String>, List<String>>() {
            public Tuple2<List<String>, List<String>> call(List<String> d) {
                List key = new ArrayList<String>(compCols.size());
                for (int i = 0; i < compCols.size(); i++) {
                    key.add(i, d.get(compCols.get(i).getColId()));
                }

                List values = new ArrayList<String>(d.size() - compCols.size());
                int valId = 0;
                for (Integer i = 0; i < d.size(); i++) {
                    boolean contain = false;
                    for (CompareCol compCol : compCols) {
                        if (compCol.getColId().equals(i)) {
                            contain = true;
                            break;
                        }
                    }
                    if (!contain) {
                        values.add(valId, d.get(i));
                        valId++;
                    }
                }
                return new Tuple2<List<String>, List<String>>(key, values);
            }
        });
    }
}