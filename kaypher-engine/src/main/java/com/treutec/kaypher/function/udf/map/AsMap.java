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

package com.treutec.kaypher.function.udf.map;

import com.treutec.kaypher.function.udf.Udf;
import com.treutec.kaypher.function.udf.UdfDescription;
import com.treutec.kaypher.function.udf.UdfParameter;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@UdfDescription(name = "AS_MAP", description = "Construct a list based on some inputs")
public class AsMap {

  @Udf
  public final <T> Map<String, T> asMap(
      @UdfParameter final List<String> keys,
      @UdfParameter final List<T> values) {
    final Map<String, T> map = new HashMap<>();
    for (int i = 0; i < keys.size(); i++) {
      final String key = keys.get(i);
      final T value = i >= values.size() ? null : values.get(i);

      map.put(key, value);
    }
    return map;
  }

}