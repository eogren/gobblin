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

package gobblin.converter.avro;

import java.io.IOException;
import java.util.Map;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.output.ByteArrayOutputStream;

import com.google.common.base.Charsets;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.MetadataAwareConverter;
import gobblin.converter.SchemaConversionException;
import gobblin.converter.SingleRecordIterable;
import gobblin.type.RecordWithMetadata;


/**
 * Converts an Avro record to a json string.
 */
public class AvroToJsonStringConverter extends Converter<Schema, String, RecordWithMetadata<GenericRecord>, RecordWithMetadata<String>> implements MetadataAwareConverter<String> {

  private Schema schema;

  private final ThreadLocal<Serializer> serializer = new ThreadLocal<Serializer>() {
    @Override
    protected Serializer initialValue() {
      return new Serializer(AvroToJsonStringConverter.this.schema);
    }
  };

  private static class Serializer {

    private final Encoder encoder;
    private final GenericDatumWriter<GenericRecord> writer;
    private final ByteArrayOutputStream outputStream;

    public Serializer(Schema schema) {
      try {
        this.writer = new GenericDatumWriter<>(schema);
        this.outputStream = new ByteArrayOutputStream();
        this.encoder = EncoderFactory.get().jsonEncoder(schema, this.outputStream);
      } catch (IOException ioe) {
        throw new RuntimeException("Could not initialize avro json encoder.");
      }
    }

    public String serialize(GenericRecord record)
        throws IOException {
      this.outputStream.reset();
      this.writer.write(record, this.encoder);
      this.encoder.flush();
      return this.outputStream.toString(Charsets.UTF_8);
    }
  }

  @Override
  public String convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    this.schema = inputSchema;
    return this.schema.toString();
  }

  @Override
  public Iterable<RecordWithMetadata<String>> convertRecord(String outputSchema,
      RecordWithMetadata<GenericRecord> inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      String jsonString = this.serializer.get().serialize(inputRecord.getRecord());
      Map<String, Object> metadata = inputRecord.getMetadata();

      if (!metadata.containsKey("Content-Type")) {
        metadata.put("Content-Type", schema.getFullName() + "+json");
      } else {
        metadata.put("Transfer-Encoding", "avro_to_json");
      }
      return new SingleRecordIterable<>(new RecordWithMetadata<>(jsonString, metadata));
    } catch (IOException ioe) {
      throw new DataConversionException(ioe);
    }
  }
}
