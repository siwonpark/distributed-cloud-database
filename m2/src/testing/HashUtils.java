package testing;

import org.apache.log4j.Logger;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;
import java.util.Base64;

public class HashUtils {

    public static String computeHash(String input){
        Logger logger = Logger.getRootLogger();
        try {
            MessageDigest md5 = MessageDigest.getInstance("MD5");
            md5.reset();
            byte[] hash = md5.digest(input.getBytes());
            return Base64.getEncoder().encodeToString(hash);
        } catch (NoSuchAlgorithmException e) {
            // This should not happen as md5 exists
            logger.error("MD5 Hash Algorithm Exception");
            return null;
        }
    }

}
