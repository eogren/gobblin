package gobblin.crypto;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import gobblin.type.SerializedRecord;
import gobblin.type.SerializedRecordWithMetadata;
import org.apache.commons.lang.RandomStringUtils;
import org.openjdk.jmh.annotations.Benchmark;
import org.openjdk.jmh.annotations.Fork;
import org.openjdk.jmh.annotations.Scope;
import org.openjdk.jmh.annotations.State;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.nio.charset.Charset;
import java.util.Map;
import java.util.Random;

public class JsonBenchmark {
    @State(Scope.Thread)
    public static class JsonBenchmarkState {
        public SerializedRecordWithMetadata serializedRecordWithMetadata;

        public JsonBenchmarkState() {
            Random r = new Random();
            String serializedVal = RandomStringUtils.randomAlphanumeric(100 + r.nextInt(1000));
            SerializedRecord innerRecord = new SerializedRecord(
                    ByteBuffer.wrap(serializedVal.getBytes(Charset.forName("UTF-8"))),
                    ImmutableList.of("text/plain")
            );

            Map<String, Object> metadata = ImmutableMap.<String, Object>of("foo", "bar");
            serializedRecordWithMetadata = new SerializedRecordWithMetadata(innerRecord, metadata);
        }
    }

    @Benchmark
    public String testToGson(JsonBenchmarkState state) {
        return state.serializedRecordWithMetadata.jsonFromGson();
    }

    @Benchmark
    public String testToGsonStreamin(JsonBenchmarkState state) throws IOException {
        return state.serializedRecordWithMetadata.jsonFromStreamingGson();
    }

    @Benchmark
    public byte[] testJacksonStreaming(JsonBenchmarkState state) throws IOException {
        return state.serializedRecordWithMetadata.jsonFromJackson();
    }
    @Benchmark
    public String testJacksonStreamingBackToString(JsonBenchmarkState state) throws IOException {
        return new String(state.serializedRecordWithMetadata.jsonFromJackson(), Charset.forName("UTF-8"));
    }
}
