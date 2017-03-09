package gobblin.runtime.crypto;

import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.commons.io.IOUtils;

import com.google.common.collect.ImmutableMap;

import gobblin.annotation.Alias;
import gobblin.crypto.EncryptionConfigParser;
import gobblin.crypto.EncryptionFactory;
import gobblin.runtime.cli.CliApplication;


@Alias(value = "decrypt", description = "Decryption utilities")
public class DecryptCli implements CliApplication {
  private static final Option KEYSTORE_LOCATION =
      Option.builder("k").longOpt("ks_location").hasArg().required().desc("Keystore location").build();
  private static final Option INPUT_LOCATION =
      Option.builder("i").longOpt("in").hasArg().required().desc("File to be decrypted").build();
  private static final Option OUTPUT_LOCATION =
      Option.builder("o").longOpt("out").hasArg().required().desc("Output file (stdin if not specified)").build();

  @Override
  public void run(String[] args) {
    try {
      Options options = new Options();
      options.addOption(KEYSTORE_LOCATION);
      options.addOption(INPUT_LOCATION);
      options.addOption(OUTPUT_LOCATION);

      CommandLine cli;
      try {
        CommandLineParser parser = new DefaultParser();
        cli = parser.parse(options, Arrays.copyOfRange(args, 1, args.length));
      } catch (ParseException pe) {
        System.out.println("Command line parse exception: " + pe.getMessage());
        printUsage(options);
        return;
      }

      Map<String, Object> encryptionProperties = ImmutableMap.<String, Object>of(
          EncryptionConfigParser.ENCRYPTION_ALGORITHM_KEY, "rotating_aes",
          EncryptionConfigParser.ENCRYPTION_KEYSTORE_PATH_KEY, cli.getOptionValue(KEYSTORE_LOCATION.getOpt()),
          EncryptionConfigParser.ENCRYPTION_KEYSTORE_PASSWORD_KEY, JCEKSKeystoreCredentialStoreCli.getPasswordFromConsole()
      );

      InputStream fIs = new BufferedInputStream(new FileInputStream(new File(cli.getOptionValue(INPUT_LOCATION.getOpt()))));
      InputStream cipherStream = EncryptionFactory.buildStreamCryptoProvider(encryptionProperties).decodeInputStream(fIs);

      OutputStream out = new BufferedOutputStream(new FileOutputStream(cli.getOptionValue(OUTPUT_LOCATION.getOpt())));

      long bytes = IOUtils.copyLarge(cipherStream, out);
      out.flush();
      out.close();
      System.out.println("Copied " + String.valueOf(bytes) + " bytes.");
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  private void printUsage(Options options) {

    HelpFormatter formatter = new HelpFormatter();

    String usage = "decryption utilities ";
    formatter.printHelp(usage, options);
  }
}
