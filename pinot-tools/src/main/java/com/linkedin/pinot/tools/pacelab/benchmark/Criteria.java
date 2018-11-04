/**
 * Copyright (C) 2014-2016 LinkedIn Corp. (pinot-core@linkedin.com)
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *         http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.linkedin.pinot.tools.pacelab.benchmark;

import java.util.Properties;

import org.apache.commons.lang.math.LongRange;
import org.xerial.util.ZipfRandom;

public class Criteria
{
	private Long maxApplyStartTime;
	private Long minApplyStartTime;
	private int queryType;
	private int secsInDuration;
	private ZipfRandom zipfRandom;

	public Criteria(Properties config, String maxStartTime, String minStartTime) {
		minApplyStartTime = Long.parseLong(config.getProperty(minStartTime));
		maxApplyStartTime = Long.parseLong(config.getProperty(maxStartTime));
		queryType = Integer.parseInt(config.getProperty(Constant.QUERY_TYPE));
		secsInDuration = Constant.HOURSECOND;
		switch(queryType) {
			case 2 : secsInDuration *= 7;
			case 1 : secsInDuration *= 24;
		}
		double zipfS = Double.parseDouble(config.getProperty(Constant.ZIPFS_PARAMETER));
		int count = (int) Math.ceil((maxApplyStartTime-minApplyStartTime)/(secsInDuration));
		zipfRandom = new ZipfRandom(zipfS,count);
	}

	private LongRange getTimeRange() {
		int duration = zipfRandom.nextInt();
		long queriedEndTime = maxApplyStartTime;
		long queriedStartTime = Math.max(minApplyStartTime,queriedEndTime - duration*secsInDuration);
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