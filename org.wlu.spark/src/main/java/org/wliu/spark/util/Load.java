package org.wliu.spark.util;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.spark.api.java.function.Function;

/**
 * Company : Altic - LIPN
 * User: Tugdual Sarazin
 * Date: 18/04/14
 * Time: 16:15
 */
public class Load {
    public static JavaRDD<List<String>> run(JavaSparkContext ctx, String filePath, final String regex) throws Exception {
        JavaRDD<String> rawData = ctx.textFile(filePath);

        return rawData.map(new Function<String, List<String>>() {
            public List<String> call(String line) {
                return Arrays.asList(line.split(regex));
            }
        });
    }
}