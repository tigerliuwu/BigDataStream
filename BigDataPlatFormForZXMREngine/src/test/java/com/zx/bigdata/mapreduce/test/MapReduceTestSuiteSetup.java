package com.zx.bigdata.mapreduce.test;

import static org.junit.Assert.assertTrue;

import org.junit.Before;
import org.junit.Test;

import com.zx.bigdata.mapreduce.test.model.HBaseClusterCache;

public class MapReduceTestSuiteSetup {

	@Test
	public void test() {
		assertTrue(true);
	}

	@Before
	public void setup() throws Exception {
		HBaseClusterCache.startupMinicluster();
	}

}
