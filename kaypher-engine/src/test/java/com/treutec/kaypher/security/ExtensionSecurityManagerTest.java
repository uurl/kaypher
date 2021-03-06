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
package com.treutec.kaypher.security;

import com.treutec.kaypher.function.udf.PluggableUdf;
import java.io.IOException;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class ExtensionSecurityManagerTest {

  private final SecurityManager securityManager = System.getSecurityManager();

  @Before
  public void before() {
    System.setSecurityManager(ExtensionSecurityManager.INSTANCE);
  }

  @After
  public void after() {
    System.setSecurityManager(securityManager);
  }

  @Test
  public void shouldAllowExec() {
    ExtensionSecurityManager.INSTANCE.checkExec("cmd");
  }

  @Test
  public void shouldAllowExit() {
    ExtensionSecurityManager.INSTANCE.checkExit(0);
  }

  @Test
  public void shouldAllowAccept() {
    ExtensionSecurityManager.INSTANCE.checkAccept("host", 90);
  }

  @Test
  public void shouldAllowConnect() {
    ExtensionSecurityManager.INSTANCE.checkConnect("host", 90);
  }

  @Test
  public void shouldAllowListen() {
    ExtensionSecurityManager.INSTANCE.checkListen(90);
  }

  @Test(expected = SecurityException.class)
  public void shouldNotAllowExecWhenPluggableUDF() throws NoSuchMethodException {
    new PluggableUdf(
        (thiz,args) -> exec(),
        new Object())
        .evaluate();
  }

  @SuppressWarnings("WeakerAccess")
  public static Process exec() {
    try {
      return Runtime.getRuntime().exec("cmd");
    } catch (IOException e) {
      return null;
    }
  }

  @Test(expected = SecurityException.class)
  public void shouldNotAllowExitWhenPluggableUDF() throws NoSuchMethodException {
    new PluggableUdf((thiz,args) -> {
      System.exit(1);
      return null;
    }, new Object()).evaluate();
  }
  
}