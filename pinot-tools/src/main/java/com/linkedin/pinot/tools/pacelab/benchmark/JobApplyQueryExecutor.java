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

public class JobApplyQueryExecutor extends QueryExecutor {
    private static JobApplyQueryExecutor instance;
    private final String CONFIG_FILE = "pinot_benchmark/query_generator_config/JobApplyConfig.properties";
    private static final String[] QUERIES = getQueries();

    private static String[] getQueries() {
        String[] queries = {
                "SELECT * FROM JobApply " +
                        "WHERE ApplyStartTime > %d AND ApplyStartTime < d% limit %d",
                "?",
                "?",
                "?"
        };
        return queries;
    }

    public String getConfigFile() {
        return CONFIG_FILE;
    }

    public static JobApplyQueryExecutor getInstance() {
        if (instance == null)
            instance = new JobApplyQueryExecutor();
        return instance;
    }

    public JobApplyQueryTask getTask(Properties config) {
        return new JobApplyQueryTask(config, QUERIES);
    }
}