package org.wliu.spark.util;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;

/**
 * Company : Altic - LIPN
 * User: Tugdual Sarazin
 * Date: 22/04/14
 * Time: 16:20
 */
public class Sort {
    @Deprecated
    public static JavaPairRDD<List<String>, List<String>> run(final JavaRDD<List<String>> dataM, final List<Integer> colsId, boolean ascending) throws Exception {
        JavaPairRDD<List<String>, List<String>> pairDataM = KeyBy.run(dataM, colsId);
        return pairDataM.sortByKey(new SortComparator(), ascending);
    }

    public static JavaPairRDD<List<String>, List<String>> run(final JavaRDD<List<String>> dataM, final List<CompareCol> compCols) throws Exception {
        JavaPairRDD<List<String>, List<String>> pairDataM = KeyByCompareCol.run(dataM, compCols);
        return pairDataM.sortByKey(new SortComparator2(compCols));
    }
}
