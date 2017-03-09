package gobblin.crypto;

import java.util.Map;

import gobblin.writer.StreamCodec;


public interface EncryptionProvider {
  /**
   * Build a StreamEncryptor with the given configuration parameters. If the provider
   * cannot satisfy the request it should return null.
   */
  StreamCodec buildStreamCryptoProvider(String algorithm, Map<String, Object> parameters);
}
