package org.wliu.spark.test.demo;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.apache.spark.api.java.JavaRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.wliu.spark.util.Filter;
import org.wliu.spark.util.FilterObject;
import org.wliu.spark.util.FilterObject.LogicOp;

import junit.framework.TestCase;

public class FilterTest extends TestCase {

    public void testRun() throws Exception {
        JavaSparkContext ctx = new org.apache.spark.api.java.JavaSparkContext("local", this.getName());

        ArrayList<List<String>> dataM = new ArrayList<List<String>>();
        dataM.add(Arrays.asList("toto", "aac", "1"));
        dataM.add(Arrays.asList("toto", "aaa", "2"));
        dataM.add(Arrays.asList("aaaA", "aaa", "30"));
        dataM.add(Arrays.asList("aaaA", "aaa", "35"));
        dataM.add(Arrays.asList("aaaB", "aaa", "37"));
        dataM.add(Arrays.asList("aaaB", "aaa", "40"));

        JavaRDD<List<String>> rddDataM = ctx.parallelize(dataM);

        // Test alpha
        FilterObject fEqTotoCol0 = new FilterObject(0, FilterObject.Operator.EQUAL, "toto", false);
        FilterObject fDiffTotoCol0 = new FilterObject(0, FilterObject.Operator.DIFF, "toto", false);
        FilterObject fInfEqAaaBCol0 = new FilterObject(0, FilterObject.Operator.INF_EQUAL, "aaaB", false);
        FilterObject fInfAaaBCol0 = new FilterObject(0, FilterObject.Operator.INF, "aaaB", false);
        FilterObject fSupAaaCol1 = new FilterObject(1, FilterObject.Operator.SUP, "aaa", false);

        List<List<String>> alpha1 = Filter.run(rddDataM, Arrays.asList(fEqTotoCol0)).collect();
        assertEquals(2, alpha1.size());
        assertTrue(alpha1.contains(Arrays.asList("toto", "aac", "1")));
        assertTrue(alpha1.contains(Arrays.asList("toto", "aaa", "2")));

        List<List<String>> alpha2 = Filter.run(rddDataM, Arrays.asList(fDiffTotoCol0)).collect();
        assertEquals(4, alpha2.size());
        assertTrue(alpha2.contains(Arrays.asList("aaaA", "aaa", "30")));
        assertTrue(alpha2.contains(Arrays.asList("aaaA", "aaa", "35")));
        assertTrue(alpha2.contains(Arrays.asList("aaaB", "aaa", "37")));
        assertTrue(alpha2.contains(Arrays.asList("aaaB", "aaa", "40")));

        List<List<String>> alpha3 = Filter.run(rddDataM, Arrays.asList(fInfEqAaaBCol0)).collect();
        assertEquals(4, alpha3.size());
        assertTrue(alpha3.contains(Arrays.asList("aaaA", "aaa", "30")));
        assertTrue(alpha3.contains(Arrays.asList("aaaA", "aaa", "35")));
        assertTrue(alpha3.contains(Arrays.asList("aaaB", "aaa", "37")));
        assertTrue(alpha3.contains(Arrays.asList("aaaB", "aaa", "40")));

        List<List<String>> alpha4 = Filter.run(rddDataM, Arrays.asList(fInfAaaBCol0)).collect();
        assertEquals(2, alpha4.size());
        assertTrue(alpha4.contains(Arrays.asList("aaaA", "aaa", "30")));
        assertTrue(alpha4.contains(Arrays.asList("aaaA", "aaa", "35")));

        List<List<String>> alpha5 = Filter.run(rddDataM, Arrays.asList(fEqTotoCol0, fSupAaaCol1)).collect();
        assertEquals(1, alpha5.size());
        assertTrue(alpha5.contains(Arrays.asList("toto", "aac", "1")));

        // Test numeric
        FilterObject fNumEq30Col2 = new FilterObject(2, FilterObject.Operator.EQUAL, "30", true);
        FilterObject fNumInf30Col2 = new FilterObject(2, FilterObject.Operator.INF, "30", true);
        FilterObject fNumSuqEq30Col2 = new FilterObject(2, FilterObject.Operator.SUP_EQUAL, "30", true);
        FilterObject fNumSuq30Col2 = new FilterObject(2, FilterObject.Operator.SUP, "30", true);
        FilterObject fNumInfEq37Col2 = new FilterObject(2, FilterObject.Operator.INF_EQUAL, "37", true);

        List<List<String>> num1 = Filter.run(rddDataM, Arrays.asList(fNumEq30Col2)).collect();
        assertEquals(1, num1.size());
        assertTrue(num1.contains(Arrays.asList("aaaA", "aaa", "30")));

        List<List<String>> num2 = Filter.run(rddDataM, Arrays.asList(fNumInf30Col2)).collect();
        assertEquals(2, num2.size());
        assertTrue(num2.contains(Arrays.asList("toto", "aac", "1")));
        assertTrue(num2.contains(Arrays.asList("toto", "aaa", "2")));

        List<List<String>> num3 = Filter.run(rddDataM, Arrays.asList(fNumSuqEq30Col2)).collect();
        assertEquals(4, num3.size());
        assertTrue(num3.contains(Arrays.asList("aaaA", "aaa", "30")));
        assertTrue(num3.contains(Arrays.asList("aaaA", "aaa", "35")));
        assertTrue(num3.contains(Arrays.asList("aaaB", "aaa", "37")));
        assertTrue(num3.contains(Arrays.asList("aaaB", "aaa", "40")));

        List<List<String>> num4 = Filter.run(rddDataM, Arrays.asList(fNumSuq30Col2, fNumInfEq37Col2)).collect();
        assertEquals(2, num4.size());
        assertTrue(num4.contains(Arrays.asList("aaaA", "aaa", "35")));
        assertTrue(num4.contains(Arrays.asList("aaaB", "aaa", "37")));

        // AlphaNum
        List<List<String>> alphaNum = Filter.run(rddDataM, Arrays.asList(fInfAaaBCol0, fNumSuq30Col2)).collect();
        assertEquals(1, alphaNum.size());
        assertTrue(alphaNum.contains(Arrays.asList("aaaA", "aaa", "35")));
        
        // emptyp filter
        List<List<String>> numFull = Filter.run(rddDataM, null).collect();
        assertEquals(6,numFull.size());
        assertTrue(numFull.contains(Arrays.asList("toto", "aac", "1")));
        assertTrue(numFull.contains(Arrays.asList("toto", "aaa", "2")));
        assertTrue(numFull.contains(Arrays.asList("aaaA", "aaa", "30")));
        assertTrue(numFull.contains(Arrays.asList("aaaA", "aaa", "35")));
        assertTrue(numFull.contains(Arrays.asList("aaaB", "aaa", "37")));
        assertTrue(numFull.contains(Arrays.asList("aaaB", "aaa", "40")));
        
        // $2 > 30 && $0= "aaaA" || $0="toto" && $1="aaa"
        FilterObject fInfCol2 = new FilterObject(2, FilterObject.Operator.SUP, "30", true);
        FilterObject fEqCol0 = new FilterObject(0, FilterObject.Operator.EQUAL, "aaaA", false, LogicOp.AND);
        FilterObject fEqtotoCol0 = new FilterObject(0, FilterObject.Operator.EQUAL, "toto", false, LogicOp.OR);
        FilterObject fEqAAACol1 = new FilterObject(1, FilterObject.Operator.EQUAL, "aaa", false);
        List<List<String>> multiFilterLogic = Filter.run(rddDataM, Arrays.asList(fInfCol2, fEqCol0,fEqtotoCol0,fEqAAACol1)).collect();
        assertEquals(2,multiFilterLogic.size());
        assertTrue(multiFilterLogic.contains(Arrays.asList("toto", "aaa", "2")));
        assertTrue(multiFilterLogic.contains(Arrays.asList("aaaA", "aaa", "35")));
        
    }
}