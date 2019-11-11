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
package com.treutec.kaypher.metastore;

import com.treutec.kaypher.function.FunctionRegistry;
import com.treutec.kaypher.metastore.model.DataSource;
import com.treutec.kaypher.name.SourceName;
import java.util.Map;
import java.util.Set;

public interface MetaStore extends FunctionRegistry, TypeRegistry {

  DataSource<?> getSource(SourceName sourceName);

  Map<SourceName, DataSource<?>> getAllDataSources();

  Set<String> getQueriesWithSource(SourceName sourceName);

  Set<String> getQueriesWithSink(SourceName sourceName);

  MetaStore copy();
}
