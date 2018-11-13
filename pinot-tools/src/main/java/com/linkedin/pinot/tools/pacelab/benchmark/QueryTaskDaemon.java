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

import java.util.concurrent.atomic.AtomicBoolean;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class QueryTaskDaemon extends QueryTask {

	private static final Logger LOGGER = LoggerFactory.getLogger(QueryTaskDaemon.class);
	protected  AtomicBoolean[] threadStatus;
	protected int threadId = -1;

	public void setThreadStatus(AtomicBoolean[] threadStatus) {
		this.threadStatus = threadStatus;
	}

	@Override
	public void run() {
		if(threadId ==-1) {
			super.run();
			return;
		}

		long timeBeforeSendingQuery;
		long timeAfterSendingQuery;
		long timeDistance;
		long runStartMillisTime = System.currentTimeMillis();
		long currentTimeMillisTime =  System.currentTimeMillis();
		long secondsPassed = (currentTimeMillisTime-runStartMillisTime)/1000;
		while(secondsPassed < _testDuration && !Thread.interrupted())
		{
			try {
				if(threadStatus[threadId].get()) {
					timeBeforeSendingQuery = System.currentTimeMillis();
					//TODO: Currently Hardcoded , will modify later on.
					generateAndRunQuery(rand.nextInt(5));
					timeAfterSendingQuery = System.currentTimeMillis();
					timeDistance = timeAfterSendingQuery - timeBeforeSendingQuery;
					if (timeDistance < 1000) {
						Thread.sleep(1000 - timeDistance);
					}
				}
				else
					Thread.sleep(500);

				currentTimeMillisTime =  System.currentTimeMillis();
				secondsPassed = (currentTimeMillisTime-runStartMillisTime)/1000;
			} catch (Exception e) {
				LOGGER.error("Exception in thread");
			}

		}
	}

	public void setThreadId(int threadId) {
		this.threadId = threadId;
	}
}
