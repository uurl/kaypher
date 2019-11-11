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

package com.treutec.kaypher.function;

import com.treutec.kaypher.util.KaypherException;

public interface MutableFunctionRegistry extends FunctionRegistry {

  /**
   * Ensure the supplied function factory is registered.
   *
   * <p>The method will register the factory if a factory with the same name is not already
   * registered. If a factory with the same name is already registered the method will throw
   * if the two factories not are equivalent, (see {@link UdfFactory#matches(UdfFactory)}.
   *
   * @param factory the factory to register.
   * @return the udf factory.
   * @throws KaypherException if a UDAF function with the same name exists, or if an incompatible UDF
   *     function factory already exists.
   */
  UdfFactory ensureFunctionFactory(UdfFactory factory);

  /**
   * Register the supplied {@code kaypherFunction}.
   *
   * <p>Note: a suitable function factory must already have been register via
   * {@link #ensureFunctionFactory(UdfFactory)}.
   *
   * @param kaypherFunction the function to register.
   * @throws KaypherException if a function, (of any type), with the same name exists.
   */
  void addFunction(KaypherFunction kaypherFunction);

  /**
   * Register an aggregate function factory.
   *
   * @param aggregateFunctionFactory the factory to register.
   * @throws KaypherException if a function, (of any type), with the same name exists.
   */
  void addAggregateFunctionFactory(AggregateFunctionFactory aggregateFunctionFactory);
}
