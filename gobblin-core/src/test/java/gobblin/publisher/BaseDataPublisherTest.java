/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package gobblin.publisher;

import java.io.IOException;

import org.apache.hadoop.fs.Path;
import org.joda.time.DateTime;
import org.joda.time.DateTimeUtils;
import org.joda.time.DateTimeZone;
import org.testng.Assert;
import org.testng.annotations.AfterClass;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.WorkUnitState;


public class BaseDataPublisherTest {
  private final static DateTime tzBoundary = new DateTime(2017, 2, 1, 2, 0,
      32, DateTimeZone.UTC);

  @BeforeClass
  public void setFixedClock() {
    DateTimeUtils.setCurrentMillisFixed(tzBoundary.getMillis());
  }

  @AfterClass
  public void resetFixedClock() {
    DateTimeUtils.setCurrentMillisSystem();
  }

  @Test
  public void testDoesNotAppendByDefault()
      throws IOException {
    WorkUnitState s = buildDefaultState();

    BaseDataPublisher publisher = new BaseDataPublisher(s.getJobState());
    Path outputPath = publisher.getFinalOutputDir(s, 0);
    Assert.assertEquals(outputPath.toString(), "/tmp/someplace");
  }

  @Test
  public void testTranslatesTimezones()
      throws IOException {
    WorkUnitState s = buildDefaultState();
    s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_TIMESTAMP, "true");
    s.setProp(ConfigurationKeys.DATA_PUBLISHER_TIMEZONE, "US/Pacific");

    // It is still 1-31 PST
    {
      BaseDataPublisher publisher = new BaseDataPublisher(s.getJobState());
      Path outputPath = publisher.getFinalOutputDir(s, 0);
      Assert.assertEquals(outputPath.toString(), "/tmp/someplace/2017-01-31");
    }

    s.setProp(ConfigurationKeys.DATA_PUBLISHER_TIMEZONE, "UTC");

    // But 2-01 UTC
    {
      BaseDataPublisher publisher = new BaseDataPublisher(s.getJobState());
      Path outputPath = publisher.getFinalOutputDir(s, 0);
      Assert.assertEquals(outputPath.toString(), "/tmp/someplace/2017-02-01");
    }
  }

  @Test
  public void testCustomizingFormatString()
      throws IOException {
    WorkUnitState s = buildDefaultState();
    s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_TIMESTAMP, "true");
    s.setProp(ConfigurationKeys.DATA_PUBLISHER_TIMEZONE, "US/Pacific");
    s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_TIMESTAMP_FORMAT, "yyyy-MM-dd-HH-mm-ss");
    BaseDataPublisher publisher = new BaseDataPublisher(s.getJobState());
    Path outputPath = publisher.getFinalOutputDir(s, 0);
    Assert.assertEquals(outputPath.toString(), "/tmp/someplace/2017-01-31-18-00-32");
  }

  private WorkUnitState buildDefaultState() {
    WorkUnitState s = new WorkUnitState();
    s.setProp(ConfigurationKeys.DATA_PUBLISHER_FINAL_DIR, "/tmp/someplace");
    s.setProp(ConfigurationKeys.DATA_PUBLISHER_APPEND_EXTRACT_TO_FINAL_DIR, "false");
    return s;
  }
}
