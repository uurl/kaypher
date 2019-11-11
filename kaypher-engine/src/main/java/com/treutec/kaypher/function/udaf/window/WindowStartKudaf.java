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
package com.treutec.kaypher.function.udaf.window;

import com.treutec.kaypher.execution.function.udaf.window.WindowSelectMapper;
import com.treutec.kaypher.function.udaf.TableUdaf;
import com.treutec.kaypher.function.udaf.UdafDescription;
import com.treutec.kaypher.function.udaf.UdafFactory;
import com.treutec.kaypher.function.udaf.placeholder.PlaceholderTableUdaf;
import com.treutec.kaypher.util.KaypherConstants;

/**
 * A placeholder KUDAF for extracting window start times.
 *
 * <p>The KUDAF itself does nothing. It's just a placeholder.
 *
 * @see WindowSelectMapper
 */
@SuppressWarnings("WeakerAccess") // Invoked via reflection.
@UdafDescription(name = "WindowStart", author = KaypherConstants.CONFLUENT_AUTHOR,
    description = "Returns the window start time, in milliseconds, for the given record. "
        + "If the given record is not part of a window the function will return NULL.")
public final class WindowStartKudaf {

  private WindowStartKudaf() {
  }

  static String getFunctionName() {
    return WindowSelectMapper.WINDOW_START_NAME;
  }

  @UdafFactory(description = "Extracts the window start time")
  public static TableUdaf<Long, Long, Long> createWindowStart() {
    return PlaceholderTableUdaf.INSTANCE;
  }
}
