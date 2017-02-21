/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package gobblin.converter;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.nio.charset.Charset;

import lombok.extern.slf4j.Slf4j;

import gobblin.configuration.WorkUnitState;
import gobblin.type.SerializedRecord;
import gobblin.writer.StreamCodec;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;


/**
 * A converter that converts a {@link SerializedRecord} to a JSON String.
 */
@Slf4j
public class SerializedRecordToJsonBytesConverter extends Converter<String, String, SerializedRecord, byte[]> {
  private StreamCodec encryptor;
  private boolean useJackson = false;
  private static final JsonFactory jsonFactory = new JsonFactory();
  @Override
  public Converter<String, String, SerializedRecord, byte[]> init(WorkUnitState workUnit) {
    super.init(workUnit);
    useJackson = workUnit.getPropAsBoolean("converter.jackson");
    return this;
  }

  @Override
  public String convertSchema(String inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    return "";
  }

  @Override
  public Iterable<byte[]> convertRecord(String outputSchema, SerializedRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      byte[] bytes;

      if (useJackson) {
        ByteArrayOutputStream bOs = new ByteArrayOutputStream(1024);
        JsonGenerator jsonGenerator = jsonFactory.createJsonGenerator(bOs, JsonEncoding.UTF8);
        inputRecord.jsonFromStreamingJackson(jsonGenerator);
        jsonGenerator.close();
        bytes = bOs.toByteArray();
      } else {
        bytes = inputRecord.toJsonString().getBytes(Charset.forName("UTF-8"));
      }
      return new SingleRecordIterable<>(bytes);
    } catch (IOException e) {
      throw new DataConversionException("Error serializing to json", e);
    }
  }
}
