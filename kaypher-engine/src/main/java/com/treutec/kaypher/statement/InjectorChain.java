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

package com.treutec.kaypher.statement;

import com.google.common.collect.ImmutableList;
import com.treutec.kaypher.parser.tree.Statement;
import java.util.List;

/**
 * Encapsulates a chain of injectors, ordered, into a single entity.
 */
public final class InjectorChain implements Injector {

  private final List<Injector> injectors;

  public static InjectorChain of(final Injector... injectors) {
    return new InjectorChain(injectors);
  }

  private InjectorChain(final Injector... injectors) {
    this.injectors = ImmutableList.copyOf(injectors);
  }

  @Override
  public <T extends Statement> ConfiguredStatement<T> inject(
      final ConfiguredStatement<T> statement) {
    ConfiguredStatement<T> injected = statement;
    for (Injector injector : injectors) {
      injected = injector.inject(injected);
    }
    return injected;
  }
}
