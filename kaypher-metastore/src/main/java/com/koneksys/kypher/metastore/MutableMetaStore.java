/*
 * Copyright 2019 Koneksys
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

package com.koneksys.kaypher.metastore;

import com.koneksys.kaypher.metastore.model.kaypherTopic;
import com.koneksys.kaypher.metastore.model.StructuredDataSource;
import java.util.Set;

public interface MutableMetaStore extends MetaStore {

  void putTopic(kaypherTopic topic);

  void putSource(StructuredDataSource<?> dataSource);

  void deleteTopic(String topicName);

  void deleteSource(String sourceName);

  void updateForPersistentQuery(
      String queryId,
      Set<String> sourceNames,
      Set<String> sinkNames);

  void removePersistentQuery(String queryId);

  MutableMetaStore copy();
}
