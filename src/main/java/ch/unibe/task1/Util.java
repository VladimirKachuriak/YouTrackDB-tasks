package ch.unibe.task1;

public class Util {
    /**
     * Compares two {@code byte[]} values lexicographically.
     * The value returned is identical to what would be returned by:
     *
     * @param  key1 the first {@code byte[]} to compare
     * @param  key2 the second {@code byte[]} to compare
     * @return the value {@code 0} if {@code key1 == key2};
     *         a value less than {@code 0} if {@code key1 < key2}; and
     *         a value greater than {@code 0} if {@code key1 > key2}
     */
    public static int compareByteArrays(byte[] key1, byte[] key2) {
        for (int i = 0; i < key1.length && i < key2.length; i++) {
            if (key1[i] > key2[i]) {
                return 1;
            } else if (key1[i] < key2[i]) {
                return -1;
            }
        }
        return Integer.compare(key1.length, key2.length);
    }
}
