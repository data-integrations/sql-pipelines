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

import io.cdap.pipeline.sql.app.core.interfaces.SQLJob;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

/**
 * A general implementation for any kind of SQL job.
 */
public class DefaultSQLJob implements SQLJob {
  private final List<StructuredStatement> preActions;
  private final List<StructuredStatement> actions;
  private final List<StructuredStatement> postActions;

  private DefaultSQLJob(List<StructuredStatement> preActions,
                        List<StructuredStatement> actions, List<StructuredStatement> postActions) {
    this.preActions = Collections.unmodifiableList(preActions);
    this.actions = Collections.unmodifiableList(actions);
    this.postActions = Collections.unmodifiableList(postActions);
  }

  public List<StructuredStatement> getPreActions() {
    return preActions;
  }

  public List<StructuredStatement> getActions() {
    return actions;
  }

  public List<StructuredStatement> getPostActions() {
    return postActions;
  }

  public static Builder builder() {
    return new Builder();
  }

  /**
   * A builder class for the DefaultSQLJob.
   */
  public static class Builder {
    private final List<StructuredStatement> preActions;
    private final List<StructuredStatement> actions;
    private final List<StructuredStatement> postActions;

    private Builder() {
      preActions = new ArrayList<>();
      actions = new ArrayList<>();
      postActions = new ArrayList<>();
    }

    public Builder addPreAction(StructuredStatement action) {
      preActions.add(action);
      return this;
    }

    public Builder addAction(StructuredStatement action) {
      actions.add(action);
      return this;
    }

    public Builder addPostAction(StructuredStatement action) {
      postActions.add(action);
      return this;
    }

    public Builder addJob(SQLJob job) {
      preActions.addAll(job.getPreActions());
      actions.addAll(job.getActions());
      postActions.addAll(job.getPostActions());
      return this;
    }

    public DefaultSQLJob build() {
      return new DefaultSQLJob(preActions, actions, postActions);
    }
  }
}
