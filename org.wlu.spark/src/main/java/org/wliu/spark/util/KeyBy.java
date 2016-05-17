package org.wliu.spark.util;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;

import scala.Tuple2;


public class KeyBy {
    public static JavaPairRDD<List<String>, List<String>> run(final JavaRDD<List<String>> dataM, final List<Integer> colsId) throws Exception {
        // Set value and key size here
        // Add test exception value size < 0
    	

        return dataM.mapToPair(new PairFunction<List<String>, List<String>, List<String>>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 7328626851437761019L;

			public Tuple2<List<String>, List<String>> call(List<String> d) {
            	List<String> key = null;
            	
            	if(colsId!=null && colsId.size()>0) {
	                key = new ArrayList<String>(colsId.size());
	                for (int i = 0; i < colsId.size(); i++) {
	                    key.add(i, d.get(colsId.get(i)));
	                }
            	}

                List<String> values = new ArrayList<String>(d.size() - (colsId == null? 0 : colsId.size()));
                int valId = 0;
                for (Integer i = 0; i < d.size(); i++) {
                    if (colsId == null || !colsId.contains(i)) {
                        values.add(valId, d.get(i));
                        valId++;
                    }
                }

                return new Tuple2<List<String>, List<String>>(key, values);
            }
        });
    }
}