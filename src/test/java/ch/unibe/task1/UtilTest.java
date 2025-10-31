package ch.unibe.task1;

import org.junit.jupiter.api.Test;

import static org.junit.jupiter.api.Assertions.*;

class UtilTest {

    @Test
    void compareByteArrays() {
        byte[] ba1 = {1, 2, 3, 4, 5, 6};
        byte[] ba2 = {1, 2, 3, 4, 5};

        assertEquals(1, Util.compareByteArrays(ba1, ba2));

        ba1 = new byte[]{1, -2, 3, 4, 5};
        ba2 = new byte[]{1, 2, 3, 4, 5};

        assertEquals(-1, Util.compareByteArrays(ba1, ba2));

        ba1 = new byte[]{1, 2, 3, 4, 5};
        ba2 = new byte[]{1, 2, 3, 4, 5};

        assertEquals(0, Util.compareByteArrays(ba1, ba2));
    }
}