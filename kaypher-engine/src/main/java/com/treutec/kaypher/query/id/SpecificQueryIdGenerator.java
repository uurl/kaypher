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
package com.treutec.kaypher.query.id;

import com.treutec.kaypher.util.KaypherServerException;
import javax.annotation.concurrent.NotThreadSafe;

/**
 * Returns a specific query Id identifier based on what's set. Only returns each set Id once and
 * will throw an exception if getNext() is called twice without being set.
 */
@NotThreadSafe
public class SpecificQueryIdGenerator implements QueryIdGenerator {

  private long nextId;
  private boolean alreadyUsed;

  public SpecificQueryIdGenerator() {
    this.nextId = 0L;
    this.alreadyUsed = true;
  }

  public void setNextId(final long nextId) {
    alreadyUsed = false;
    this.nextId = nextId;
  }

  @Override
  public String getNext() {
    if (alreadyUsed) {
      throw new KaypherServerException("QueryIdGenerator has not been updated with new offset");
    }

    alreadyUsed = true;
    return String.valueOf(nextId);
  }

  @Override
  public QueryIdGenerator createSandbox() {
    return new SequentialQueryIdGenerator(nextId + 1);
  }
}
