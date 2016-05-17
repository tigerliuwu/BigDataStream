package org.wliu.spark.util;
import scala.Serializable;


public class FilterObject implements Serializable {
    /**
	 * 
	 */
	private static final long serialVersionUID = 5446064124484190486L;

	public enum Operator {
        EQUAL, DIFF, SUP, SUP_EQUAL, INF, INF_EQUAL
    }

    public enum LogicOp {
        AND, OR
    }

    final public int idCol;
    final public Operator op;
    final public String value;
    final public Boolean isNumeric;
    final public LogicOp logicOp;

    public FilterObject(int idCol, Operator op, String value, Boolean isNumeric){
        this(idCol, op, value, isNumeric, LogicOp.AND);
    }

    public FilterObject(int idCol, Operator op, String value, Boolean isNumeric, LogicOp logicOp){
        if (isNumeric) {
            Double.parseDouble(value);
        }

        this.idCol = idCol;
        this.op = op;
        this.value = value;
        this.isNumeric = isNumeric;
        this.logicOp = logicOp;
    }
}