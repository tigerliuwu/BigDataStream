package com.zx.bigdata.mapreduce.test;

import static org.junit.Assert.assertTrue;

import org.junit.After;
import org.junit.Test;

import com.zx.bigdata.mapreduce.test.model.HBaseClusterCache;

public class MapReduceTestSuiteClearup {

	@Test
	public void test() {
		assertTrue(true);
	}

	@After
	public void clearup() throws Exception {
		HBaseClusterCache.shutdownMinicluster();
	}
}
