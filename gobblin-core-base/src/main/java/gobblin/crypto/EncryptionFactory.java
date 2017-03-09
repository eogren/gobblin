package gobblin.crypto;

import java.util.Map;
import java.util.ServiceLoader;

import lombok.extern.slf4j.Slf4j;

import gobblin.writer.StreamCodec;

@Slf4j
public class EncryptionFactory {
  private static ServiceLoader<EncryptionProvider> encryptionProviderLoader = ServiceLoader.load(EncryptionProvider.class);

  public static StreamCodec buildStreamCryptoProvider(Map<String, Object> parameters) {
    String encryptionType = EncryptionConfigParser.getEncryptionType(parameters);
    if (encryptionType == null) {
      throw new IllegalArgumentException("Encryption type not present in parameters!");
    }

    return buildStreamCryptoProvider(encryptionType, parameters);
  }

  /**
   * Return a StreamEncryptor for the given algorithm and with appropriate parameters.
   * @param algorithm ALgorithm to build
   * @param parameters Parameters for algorithm
   * @return A SreamEncoder for that algorithm
   * @throws IllegalArgumentException If the given algorithm/parameter pair cannot be built
   */
  public static StreamCodec buildStreamCryptoProvider(String algorithm, Map<String, Object> parameters) {
    for (EncryptionProvider provider: encryptionProviderLoader) {
      log.debug("Looking for algorithm {} in provider {}", algorithm, provider.getClass().getName());
      StreamCodec codec = provider.buildStreamCryptoProvider(algorithm, parameters);
      if (codec != null) {
        log.debug("Found algorithm {} in provider {}", algorithm, provider.getClass().getName());
        return codec;
      }
    }

    throw new IllegalArgumentException("Could not find a provider to build algorithm " + algorithm);
  }
}
