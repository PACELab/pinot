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
	private Long maxStartTime;
	private Long minStartTime;
	private int queryType;
	private int secsInDuration;
	private ZipfRandom zipfRandom;

	public Criteria(Properties config, String pMaxStartTime, String pMinStartTime) {
		minStartTime = Long.parseLong(config.getProperty(pMinStartTime));
		maxStartTime = Long.parseLong(config.getProperty(pMaxStartTime));
		queryType = Integer.parseInt(config.getProperty(Constant.QUERY_TYPE));
		switch(queryType) {
			case 1 : secsInDuration = Constant.HOURSECOND * 24;
					break;
			case 2 : secsInDuration = Constant.HOURSECOND * 7 * 24;
					break;
			default : secsInDuration = Constant.HOURSECOND;
					break;

		}
		double zipfS = Double.parseDouble(config.getProperty(Constant.ZIPFS_PARAMETER));
		int count = (int) Math.ceil((maxStartTime-minStartTime)/(secsInDuration));
		zipfRandom = new ZipfRandom(zipfS,count);
	}

	private LongRange getTimeRange() {
		int duration = zipfRandom.nextInt();
		long queriedEndTime = maxStartTime;
		long queriedStartTime = Math.max(minStartTime,queriedEndTime - duration*secsInDuration);
		return new LongRange(queriedStartTime,queriedEndTime);
	}
	public String getClause(String column) {
		LongRange timeRange = getTimeRange();
		if (queryType == 0)
			return "";
		return column + " > " + timeRange.getMinimumLong() +" AND " + column + " < "+timeRange.getMaximumLong();

	}
}