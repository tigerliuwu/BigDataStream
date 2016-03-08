package com.zx.bigdata.mapreduce.test;

import org.junit.runner.RunWith;
import org.junit.runners.Suite;

import com.zx.bigdata.mapreduce.test.tax.TaxBasicSegmentTest;
import com.zx.bigdata.mapreduce.test.tax.TaxPunishSegmentTest;
import com.zx.bigdata.mapreduce.test.tax.TaxTypeDetailedSegmentTest;

@RunWith(Suite.class)
@Suite.SuiteClasses({ TaxBasicSegmentTest.class,

		TaxPunishSegmentTest.class, TaxTypeDetailedSegmentTest.class,
		// register new junit test here

})

public class MapReduceTestSuite4Files {

}
