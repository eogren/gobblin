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

package gobblin.type;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.StringWriter;
import java.util.List;
import java.util.Map;

import com.google.common.collect.ImmutableList;
import com.google.gson.stream.JsonWriter;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;


/**
 * A holder for serialized records with Metadata.
 */
public class SerializedRecordWithMetadata extends RecordWithMetadata<SerializedRecord> {
  public static final List<String> CONTENT_TYPE = ImmutableList.of("application/vnd.lnkd.serializedRecordWithMetadata");
  public static final List<String> CONTENT_TYPE_JSON = ImmutableList.of(CONTENT_TYPE.get(0) + "+json");
  private static final JsonFactory jacksonFactory = new JsonFactory();
  public SerializedRecordWithMetadata(SerializedRecord record, Map<String, Object> metadata) {
    super(record, metadata);
  }

  public String jsonFromGson() {
    return SerializedRecord.getSerializedAwareGson().toJson(this);
  }

  public byte[] jsonFromJackson() throws IOException {
    ByteArrayOutputStream bOs = new ByteArrayOutputStream(1024);
    JsonGenerator jsonGenerator = jacksonFactory.createJsonGenerator(bOs, JsonEncoding.UTF8);

    jsonGenerator.writeStartObject();
    jsonGenerator.writeObjectFieldStart("metadata");
    for (Map.Entry<String, Object> entry: getMetadata().entrySet()) {
      jsonGenerator.writeStringField(entry.getKey(), (String)entry.getValue());
    }
    jsonGenerator.writeEndObject();
    jsonGenerator.writeFieldName("record");
    getRecord().jsonFromStreamingJackson(jsonGenerator);
    jsonGenerator.writeEndObject();

    jsonGenerator.close();
    return bOs.toByteArray();
  }

  public String jsonFromStreamingGson() throws IOException {
    StringWriter writer = new StringWriter(1024);
    JsonWriter jsonWriter = new JsonWriter(writer);
    jsonWriter.setHtmlSafe(false);
    jsonWriter.beginObject();
    jsonWriter.name("metadata");

    // Array
    jsonWriter.beginObject();
    for (Map.Entry<String, Object> entry: getMetadata().entrySet()) {
      jsonWriter.name(entry.getKey()).value((String)entry.getValue());
    }
    jsonWriter.endObject();

    jsonWriter.name("record");
    getRecord().jsonFromStreamingGson(jsonWriter);
    jsonWriter.endObject();

    jsonWriter.close();
    return writer.toString();
 }
}
