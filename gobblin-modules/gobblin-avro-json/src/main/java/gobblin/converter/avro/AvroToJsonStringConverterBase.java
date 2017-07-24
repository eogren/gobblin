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
import java.util.Collections;

import org.apache.avro.Schema;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.Encoder;
import org.apache.avro.io.EncoderFactory;
import org.apache.commons.io.output.ByteArrayOutputStream;
import org.codehaus.jackson.JsonEncoding;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonGenerator;
import org.codehaus.jackson.util.MinimalPrettyPrinter;

import com.google.common.base.Charsets;
import com.google.common.collect.Lists;

import gobblin.configuration.WorkUnitState;
import gobblin.converter.Converter;
import gobblin.converter.DataConversionException;
import gobblin.converter.SchemaConversionException;


/**
 * Converts an Avro record to a json string.
 */
public abstract class AvroToJsonStringConverterBase<T> extends Converter<Schema, String, GenericRecord, T> {

  private Schema schema;

  private final ThreadLocal<Serializer> serializer = new ThreadLocal<Serializer>() {
    @Override
    protected Serializer initialValue() {
      return new Serializer(AvroToJsonStringConverterBase.this.schema);
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

        // Ensure that no newlines are introduced when the Serializer is re-used
        JsonGenerator jsonGenerator = new JsonFactory().createJsonGenerator(outputStream, JsonEncoding.UTF8);
        MinimalPrettyPrinter pp = new MinimalPrettyPrinter();
        pp.setRootValueSeparator("");
        jsonGenerator.setPrettyPrinter(pp);

        this.encoder = EncoderFactory.get().jsonEncoder(schema, jsonGenerator);
      } catch (IOException ioe) {
        throw new RuntimeException("Could not initialize avro json encoder.");
      }
    }

    public byte[] serialize(GenericRecord record) throws IOException {
      this.outputStream.reset();
      this.writer.write(record, this.encoder);
      this.encoder.flush();
      return this.outputStream.toByteArray();
    }
  }

  @Override
  public String convertSchema(Schema inputSchema, WorkUnitState workUnit)
      throws SchemaConversionException {
    this.schema = inputSchema;
    return this.schema.toString();
  }

  @Override
  public Iterable<T> convertRecord(String outputSchema, GenericRecord inputRecord, WorkUnitState workUnit)
      throws DataConversionException {
    try {
      byte[] utf8Bytes = this.serializer.get().serialize(inputRecord);
      return Collections.singleton(processUtf8Bytes(utf8Bytes));
    } catch (IOException ioe) {
      throw new DataConversionException(ioe);
    }
  }

  protected abstract T processUtf8Bytes(byte[] utf8Bytes);
}
