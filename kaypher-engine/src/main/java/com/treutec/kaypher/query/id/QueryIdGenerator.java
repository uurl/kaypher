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

/**
 * Generator used to provide query ids.
 */
public interface QueryIdGenerator {

  /**
   * Returns the next query id identifier.
   *
   * @return a string query id identifier
   */
  String getNext();

  /**
   * Create an generator that can be used to create new ids without affecting the state
   * of the real generator.
   *
   * @return a new generator initialized at what the next id would be
   */
  QueryIdGenerator createSandbox();
}
