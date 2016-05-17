package org.wliu.spark.util;
import java.io.Serializable;
import java.util.Comparator;
import java.util.List;

/**
 * Company : Altic - LIPN
 * User: Tugdual Sarazin
 * Date: 27/05/14
 * Time: 16:34
 */
@Deprecated
public class SortComparator implements Comparator<List<String>>, Serializable {
    @Override
    public int compare(List<String> o1, List<String> o2) {
        if (o1.size() != o2.size()) {
            return o1.size() - o2.size();
        }
        int cmp;
        for (int i= 0; i < o1.size(); i++) {
            cmp = o1.get(i).compareTo(o2.get(i));
            if (cmp != 0) {
                return cmp;
            }
        }
        return 0;
    }
}