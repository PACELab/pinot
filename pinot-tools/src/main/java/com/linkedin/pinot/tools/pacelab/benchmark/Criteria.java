package com.linkedin.pinot.tools.pacelab.benchmark;

import java.util.Properties;

import org.apache.commons.lang.math.LongRange;
import org.xerial.util.ZipfRandom;

public  class Criteria {
	
	private final int HOURSECOND = 3600;
	private Long maxApplyStartTime;
	private Long minApplyStartTime;
	private int queryType;
	private int durationSecs;
	private ZipfRandom zipfRandom;
	
	public Criteria(Properties config) {
		minApplyStartTime = Long.parseLong(config.getProperty("MinProfileViewStartTime"));
        maxApplyStartTime = Long.parseLong(config.getProperty("MaxProfileViewStartTime"));
		queryType = Integer.parseInt(config.getProperty("queryType"));
		durationSecs = HOURSECOND;
		switch(queryType) {
			case 2 : durationSecs *= 7;
			case 1 : durationSecs *= 24;
		}
		double zipfS = Double.parseDouble(config.getProperty("ZipfSParameter"));
		int count = (int) Math.ceil((maxApplyStartTime-minApplyStartTime)/(durationSecs));
		zipfRandom = new ZipfRandom(zipfS,count);
	}
	
	
		
	private LongRange getTimeRange() {
		long queriedEndTime = maxApplyStartTime - zipfRandom.nextInt()*durationSecs;
        long queriedStartTime = minApplyStartTime;
		LongRange timeRange =  new LongRange(queriedStartTime,queriedEndTime);
		return timeRange;
	}
	public String getClause(String column) {
		LongRange timeRange = getTimeRange();
		if (queryType == 0)
			return "";
		return " AND " + column + " > " + timeRange.getMinimumLong() +" AND " + column + " < timeRange.getMaximumLong() ";

	}
	

}


//public abstract class Criteria {
//	
//	abstract protected Long maxApplyStartTime();
//	abstract protected Long minApplyStartTime();
//	abstract protected int queryType();
//	abstract protected int durationSecs();
//	abstract protected ZipfRandom zipfRandom();
//	
//	protected String param() {
//		return null;
//	}
//		
//	LongRange getTimeRange() {
//		long queriedEndTime = maxApplyStartTime() - zipfRandom().nextInt()*durationSecs();
//        long queriedStartTime = minApplyStartTime();
//		LongRange timeRange =  new LongRange(queriedStartTime,queriedEndTime);
//		return timeRange;
//	}
//	String getClause() {
//		LongRange timeRange = getTimeRange();
//		if (queryType() == 0)
//			return "";
//		return " AND " + param() + " > " + timeRange.getMinimumLong() +" AND " + timeRange.getMaximumLong() + " < %d ";
//
//	}
//	
//
//}