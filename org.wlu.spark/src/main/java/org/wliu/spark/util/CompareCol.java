package org.wliu.spark.util;
import java.io.Serializable;

/**
 * Company : Altic - LIPN
 * User: Tugdual Sarazin
 * Date: 05/06/14
 * Time: 15:31
 */
public class CompareCol implements Serializable {
    private Integer colId;
    private boolean ascending;
    private CompareType type = CompareType.LETTER;
    
    public CompareCol(Integer colId, boolean ascending, CompareType type) {
        this.colId = colId;
        this.ascending = ascending;
        this.type = type;
    }

    public Integer getColId() {
        return colId;
    }

    public boolean isAscending() {
        return ascending;
    }
    
    public CompareType getCompareType() {
        return type;
    }
}