package org.wliu.spark.util;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.function.PairFunction;
import scala.Tuple2;

import java.util.ArrayList;
import java.util.List;
import com.google.common.base.Optional;


public class Join {
    /* Inner join */
    public static JavaPairRDD<List<String>, List<String>> inner(final JavaPairRDD<List<String>, List<String>> dataM1, final JavaPairRDD<List<String>, List<String>> dataM2) throws Exception {
        JavaPairRDD<List<String>, Tuple2<List<String>, List<String>>> joinData = dataM1.join(dataM2);

        return joinData.mapToPair(new PairFunction<Tuple2<List<String>, Tuple2<List<String>, List<String>>>, List<String>, List<String>>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
            public Tuple2<List<String>, List<String>> call(Tuple2<List<String>, Tuple2<List<String>, List<String>>> s) throws Exception {

                List<String> combined = new ArrayList<String>();
                combined.addAll(s._2()._1());
                combined.addAll(s._2()._2());
                return new Tuple2<List<String>, List<String>>(s._1(), combined);
            }
        });
    }

    public static JavaPairRDD<List<String>, List<String>> inner(final JavaRDD<List<String>> dataM1, final List<Integer> colsId1, final JavaPairRDD<List<String>, List<String>> dataM2) throws Exception {
        return Join.inner(KeyBy.run(dataM1, colsId1), dataM2);
    }

    public static JavaPairRDD<List<String>, List<String>> inner(final JavaRDD<List<String>> dataM1, final List<Integer> colsId1, final JavaRDD<List<String>> dataM2, final List<Integer> colsId2) throws Exception {
        return Join.inner(KeyBy.run(dataM1, colsId1), KeyBy.run(dataM2, colsId2));
    }

    /* Left join */
    public static JavaPairRDD<List<String>, List<String>> left(final JavaPairRDD<List<String>, List<String>> dataM1, final JavaPairRDD<List<String>, List<String>> dataM2) throws Exception {
        JavaPairRDD<List<String>, Tuple2<List<String>, Optional<List<String>>>> joinData = dataM1.leftOuterJoin(dataM2);

        return joinData.mapToPair(new PairFunction<Tuple2<List<String>, Tuple2<List<String>, Optional<List<String>>>>, List<String>, List<String>>() {
            @Override
            public Tuple2<List<String>, List<String>> call(Tuple2<List<String>, Tuple2<List<String>, Optional<List<String>>>> s) {
                // left and right elements
                if (s._2()._2().isPresent()) {
                    List<String> combined = new ArrayList<String>();
                    combined.addAll(s._2()._1());
                    combined.addAll(s._2()._2().get());
                    return new Tuple2<List<String>, List<String>>(s._1(), combined);
                }
                // Only left elements
                return new Tuple2<List<String>, List<String>>(s._1(), s._2()._1());
            }
        });
    }

    public static JavaPairRDD<List<String>, List<String>> left(final JavaRDD<List<String>> dataM1, final List<Integer> colsId1, final JavaPairRDD<List<String>, List<String>> dataM2) throws Exception {
        return Join.left(KeyBy.run(dataM1, colsId1), dataM2);
    }

    public static JavaPairRDD<List<String>, List<String>> left(final JavaRDD<List<String>> dataM1, final List<Integer> colsId1, final JavaRDD<List<String>> dataM2, final List<Integer> colsId2) throws Exception {
        return Join.left(KeyBy.run(dataM1, colsId1), KeyBy.run(dataM2, colsId2));
    }


    /* CoGroup */
    public static JavaPairRDD<List<String>, List<String>> cogroup(final JavaPairRDD<List<String>, List<String>> dataM1, final JavaPairRDD<List<String>, List<String>> dataM2) throws Exception {
        JavaPairRDD<List<String>, Tuple2<Iterable<List<String>>, Iterable<List<String>>>> joinData = dataM1.cogroup(dataM2);

        return joinData.flatMapToPair(new PairFunction<Tuple2<List<String>, Tuple2<List<List<String>>, List<List<String>>>>, List<String>, List<String>>() {
            /**
			 * 
			 */
			private static final long serialVersionUID = 1L;

			@Override
            public Tuple2<List<String>, List<String>> call(Tuple2<List<String>, Tuple2<List<List<String>>, List<List<String>>>> s) {
                List<String> combined = new ArrayList<String>();
                // Add left part
                for (List<String> sLeft : s._2()._1()) {
                    combined.addAll(sLeft);
                }
                // Add right part
                for (List<String> sRight : s._2()._2()) {
                    combined.addAll(sRight);
                }
                return new Tuple2<List<String>, List<String>>(s._1(), combined);
            }
        });
    }

    public static JavaPairRDD<List<String>, List<String>> cogroup(final JavaRDD<List<String>> dataM1, final List<Integer> colsId1, final JavaPairRDD<List<String>, List<String>> dataM2) throws Exception {
        return Join.cogroup(KeyBy.run(dataM1, colsId1), dataM2);
    }

    public static JavaPairRDD<List<String>, List<String>> cogroup(final JavaRDD<List<String>> dataM1, final List<Integer> colsId1, final JavaRDD<List<String>> dataM2, final List<Integer> colsId2) throws Exception {
        return Join.cogroup(KeyBy.run(dataM1, colsId1), KeyBy.run(dataM2, colsId2));
    }
}