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
package gobblin.data.management.copy.writer;

import gobblin.capability.Capability;
import gobblin.capability.CapabilityAware;
import gobblin.capability.CapabilityParser;
import gobblin.capability.CapabilityParsers;
import gobblin.configuration.ConfigurationKeys;
import gobblin.configuration.State;
import gobblin.crypto.EncryptionUtils;
import gobblin.data.management.copy.FileAwareInputStream;
import gobblin.writer.DataWriter;
import gobblin.writer.DataWriterBuilder;
import gobblin.writer.StreamEncoder;

import java.io.IOException;
import java.util.Collections;
import java.util.List;
import java.util.Map;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.fs.Path;

import com.google.common.collect.ImmutableList;


/**
 * A {@link DataWriterBuilder} for {@link FileAwareInputStreamDataWriter}
 */
public class FileAwareInputStreamDataWriterBuilder extends DataWriterBuilder<String, FileAwareInputStream> implements CapabilityAware {
  @Override
  public final DataWriter<FileAwareInputStream> build() throws IOException {
    setJobSpecificOutputPaths(this.destination.getProperties());
    // Each writer/mapper gets its own task-staging directory
    this.destination.getProperties().setProp(ConfigurationKeys.WRITER_FILE_PATH, this.writerId);
    return buildWriter();
  }

  protected DataWriter<FileAwareInputStream> buildWriter() throws IOException {
    List<StreamEncoder> encoders = getStreamEncoders();
    return new FileAwareInputStreamDataWriter(this.destination.getProperties(), this.branches, this.branch,
        this.writerAttemptId, encoders);
  }

  protected List<StreamEncoder> getStreamEncoders() {
    List<StreamEncoder> encoders = Collections.emptyList();

    CapabilityParser.CapabilityRecord encryptionInfo =
        CapabilityParsers.writerCapabilityForBranch(Capability.ENCRYPTION, this.destination.getProperties(),
            this.branches, this.branch);
    if (encryptionInfo.isConfigured()) {
      encoders = ImmutableList.of(EncryptionUtils.buildStreamEncryptor(encryptionInfo.getParameters()));
    }
    return encoders;
  }

  /**
   * Each job gets its own task-staging and task-output directory. Update the staging and output directories to
   * contain job_id. This is to make sure uncleaned data from previous execution does not corrupt final published data
   * produced by this execution.
   */
  public synchronized static void setJobSpecificOutputPaths(State state) {

    // Other tasks may have set this already
    if (!StringUtils.containsIgnoreCase(state.getProp(ConfigurationKeys.WRITER_STAGING_DIR),
        state.getProp(ConfigurationKeys.JOB_ID_KEY))) {

      state.setProp(ConfigurationKeys.WRITER_STAGING_DIR, new Path(state.getProp(ConfigurationKeys.WRITER_STAGING_DIR),
          state.getProp(ConfigurationKeys.JOB_ID_KEY)));
      state.setProp(ConfigurationKeys.WRITER_OUTPUT_DIR, new Path(state.getProp(ConfigurationKeys.WRITER_OUTPUT_DIR),
          state.getProp(ConfigurationKeys.JOB_ID_KEY)));

    }

  }

  @Override
  public boolean supportsCapability(Capability c, Map<String, Object> properties) {
    String type = (String)properties.get(Capability.ENCRYPTION_TYPE);
    return (c.equals(Capability.ENCRYPTION) && EncryptionUtils.supportedStreamingAlgorithms().contains(type));
  }
}
