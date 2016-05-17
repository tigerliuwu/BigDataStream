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
public class SortComparator2 implements Comparator<List<String>>, Serializable {

    private List<CompareCol> compCols;

    public SortComparator2(List<CompareCol> compCols) {
        this.compCols = compCols;
    }

    public int compare(List<String> o1, List<String> o2) {
        if (o1.size() != o2.size()) {
            return o1.size() - o2.size();
        }
        int cmp;
        for (int i= 0; i < o1.size(); i++) {
        	if(this.compCols.get(i).getCompareType() == CompareType.DIGIT) {
        		double i1 = Double.parseDouble(o1.get(i));
        		double i2 = Double.parseDouble(o2.get(i));
        		cmp = i1 > i2 ? 1 : (i1 == i2 ? 0 : -1);//comparing double is not a good idea.
        	} else {
        		cmp = o1.get(i).compareTo(o2.get(i));
        	}

            // o1 != o2
            if (cmp != 0) {
                boolean asc = this.compCols.get(i).isAscending();
                // o1 > o2 && asc || o1 < o2 && desc
                if ((cmp > 0 && asc) || (cmp < 0 && !asc)) {
                    return 1;
                // o1 < o2 && asc || o1 > o2 && desc
                } else {
                    return -1;
                }
            }
        }
        return 0;
    }
}