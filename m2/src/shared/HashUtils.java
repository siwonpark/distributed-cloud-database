package shared;

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

    /**
     * Check if hash is within the hash range defined by start, end
     * And handle the case where start > end (i.e. the range crosses the start of the
     * Hash ring)
     * @param hash The hash to check
     * @param start Hash range start
     * @param end Hash range end
     * @return True if contained within the hash range, false otherwise
     */
    public static boolean withinHashRange(String hash, String start, String end){
        if (start.compareTo(end) < 0) {
            return hash.compareTo(start) >= 0 && hash.compareTo(end) < 0;
        } else {
            // start is greater than end, the node is responsible for an area across the start of the ring
            return hash.compareTo(start) >= 0 || hash.compareTo(end) < 0;
        }
    }

}
