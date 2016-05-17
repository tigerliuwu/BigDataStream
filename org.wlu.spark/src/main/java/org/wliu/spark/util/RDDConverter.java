package org.wliu.spark.util;
import java.util.ArrayList;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.Function;

import scala.Tuple2;

public class RDDConverter {
	
    public static JavaRDD<List<String>> convert(final JavaRDD<Tuple2<List<String>, List<String>>> data, final List<Integer> colIds) throws Exception {
    	
        return data.map(new Function<Tuple2<List<String>, List<String>>, List<String>>() {

			public List<String> call(Tuple2<List<String>, List<String>> input)
					throws Exception {
				List<String> keys = input._1();
				List<String> valuesWithoutKey = input._2();
				List<String> output = new ArrayList<String>();
				output.addAll(valuesWithoutKey);
				if(colIds!=null && colIds.size()>0){
					for(int index=0;index<keys.size();index++){
						output.add(colIds.get(index), keys.get(index));
					}
				}else{
					output.addAll(0, keys);
				}
				return output;
			}
        });
        
    }
    public static JavaRDD<List<String>> convert(final JavaRDD<Tuple2<List<String>, List<String>>> data) throws Exception {
    	
    	return convert(data,null);
        
    }
}