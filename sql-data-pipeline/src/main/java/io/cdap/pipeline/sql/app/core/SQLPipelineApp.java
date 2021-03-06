/*
 * Copyright © 2019 Cask Data, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License"); you may not
 * use this file except in compliance with the License. You may obtain a copy of
 * the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations under
 * the License.
 */

package io.cdap.pipeline.sql.app.core;

import io.cdap.cdap.api.app.AbstractApplication;
import io.cdap.cdap.api.app.ProgramType;
import io.cdap.cdap.api.schedule.ScheduleBuilder;

/**
 * The pipeline application for an SQL pipeline.
 */
public class SQLPipelineApp extends AbstractApplication<SQLConfig> {
  public static final String SCHEDULE_NAME = "dataPipelineSchedule";
  public static final String DEFAULT_DESCRIPTION = "SQL Pipeline Application";

  @Override
  public void configure() {
    SQLConfig config = getConfig();

    if (config.getDescription() != null) {
      setDescription(config.getDescription());
    } else {
      setDescription(DEFAULT_DESCRIPTION);
    }

    getConfigurer().addWorkflow(new SQLWorkflow(config, getConfigurer()));

    String timeSchedule = config.getSchedule();
    if (timeSchedule != null) {
      ScheduleBuilder scheduleBuilder = buildSchedule(SCHEDULE_NAME, ProgramType.WORKFLOW, SQLWorkflow.NAME)
        .setDescription("Data pipeline schedule");
      schedule(scheduleBuilder.triggerByTime(timeSchedule));
    }
  }
}
