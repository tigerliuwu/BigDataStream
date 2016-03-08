package com.zx.bigdata.mapreduce.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.zx.bigdata.mapreduce.test.tax.TaxBasicSegmentDeleteTest_minicluster;
import com.zx.bigdata.mapreduce.test.tax.TaxBasicSegmentTest_minicluster;

@RunWith(Suite.class)
@Suite.SuiteClasses({ MapReduceTestSuiteSetup.class,

		TaxBasicSegmentTest_minicluster.class, TaxBasicSegmentDeleteTest_minicluster.class,
		// register new junit test here

		MapReduceTestSuiteClearup.class

})

public class MapReduceTestSuite {

}
