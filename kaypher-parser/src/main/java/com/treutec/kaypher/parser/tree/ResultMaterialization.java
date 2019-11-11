/*
 * Copyright 2019 Treu Techologies
 *
 * See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.treutec.kaypher.parser.tree;

/**
 * Controls how the result of a query is materialized.
 */
public enum ResultMaterialization {

  /**
   * All intermediate results should be materialized.
   *
   * <p>For a table, this would mean output all the events in the change log, or all the
   * intermediate aggregation results being computed in an aggregating query.
   *
   * <p>For a stream, all events are changes, so all are output.
   */
  CHANGES,

  /**
   * Only final results should be materialized.
   *
   * <p>For a table, this would mean output only the finalized result per-key.
   *
   * <p>For a stream, all events are final, so all are output.
   */
  FINAL;
}
