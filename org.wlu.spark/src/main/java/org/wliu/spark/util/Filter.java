package org.wliu.spark.util;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import java.util.List;

public class Filter {
    public static JavaRDD<List<String>> run(final JavaRDD<List<String>> dataM, final List<FilterObject> filters) throws Exception {
        return dataM.filter(new Function<List<String>, Boolean>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = -6158437531764082862L;

			public Boolean call(List<String> row) {
                Boolean result = true;
                if (filters == null || filters.isEmpty()) {
                	return result;
                }
                for (int i =0; i < filters.size(); i++) {
                	FilterObject filter = filters.get(i);
                	boolean tempResult = true;
                    switch (filter.op) {
                        case EQUAL:
                            if (filter.isNumeric) {
                                if (!(Double.parseDouble(row.get(filter.idCol)) == Double.parseDouble(filter.value))) { 
                                	tempResult = false; 
                                }
                            } else {
                                if (!(row.get(filter.idCol).equals(filter.value))) {
                                	tempResult = false; 
                                }
                            }
                            break;
                        case DIFF:
                            if (filter.isNumeric) {
                                if (Double.parseDouble(row.get(filter.idCol)) == Double.parseDouble(filter.value)) { 
                                	tempResult = false; 
                                } 
                            } else {
                                if (row.get(filter.idCol).equals(filter.value)) { 
                                	tempResult = false; 
                                }
                            }
                            break;
                        case SUP:
                            if (filter.isNumeric) {
                                if (!(Double.parseDouble(row.get(filter.idCol)) > Double.parseDouble(filter.value))) { 
                                	tempResult = false; 
                                } 
                            } else {
                                if (!(row.get(filter.idCol).compareTo(filter.value) > 0)) { 
                                	tempResult = false; 
                                } 
                            }
                            break;
                        case SUP_EQUAL:
                            if (filter.isNumeric) {
                                if (!(Double.parseDouble(row.get(filter.idCol)) >= Double.parseDouble(filter.value))) { 
                                	tempResult = false; 
                                } 
                            } else {
                                if (!(row.get(filter.idCol).compareTo(filter.value) >= 0)) { 
                                	tempResult = false; 
                                }
                            }
                            break;
                        case INF:
                            if (filter.isNumeric) {
                                if (!(Double.parseDouble(row.get(filter.idCol)) < Double.parseDouble(filter.value))) { 
                                	tempResult = false; 
                                }
                            } else {
                                if (!(row.get(filter.idCol).compareTo(filter.value) < 0)) { 
                                	tempResult = false; 
                                } 
                            }
                            break;
                        case INF_EQUAL:
                            if (filter.isNumeric) {
                                if (!(Double.parseDouble(row.get(filter.idCol)) <= Double.parseDouble(filter.value))) { 
                                	tempResult = false; 
                                } 
                            } else {
                                if (!(row.get(filter.idCol).compareTo(filter.value) <= 0)) { 
                                	tempResult = false; 
                                } 
                            }
                            break;
                    }
                    if (i==0) {
                    	result = tempResult;
                    } else {
	                    if (filter.logicOp == FilterObject.LogicOp.OR) {
	                    	result = result || tempResult;
	                    } else {
	                    	result = result && tempResult;
	                    }
                    }
                    
                }
                return result;
            }
        });
    }
}