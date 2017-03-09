package gobblin.test.crypto;

import java.util.Map;

import gobblin.crypto.EncryptionProvider;
import gobblin.writer.StreamCodec;


public class TestEncryptionProvider implements EncryptionProvider {
  private static final String INSECURE_SHIFT_TAG = InsecureShiftCodec.TAG;

  @Override
  public StreamCodec buildStreamCryptoProvider(String algorithm, Map<String, Object> parameters) {
    switch (algorithm) {
      case INSECURE_SHIFT_TAG:
        return new InsecureShiftCodec(parameters);
      default:
        return null;
    }
  }
}
