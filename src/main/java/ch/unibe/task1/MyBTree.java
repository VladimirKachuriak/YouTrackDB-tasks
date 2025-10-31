package ch.unibe.task1;

public interface MyBTree {
    void put(byte[] key, byte[] value);
    byte[] get(byte[] key);
}
