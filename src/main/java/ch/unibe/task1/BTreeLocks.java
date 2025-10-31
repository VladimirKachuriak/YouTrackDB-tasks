package ch.unibe.task1;

import lombok.Getter;
import lombok.Setter;

import java.util.Comparator;
import java.util.concurrent.locks.ReentrantReadWriteLock;

/**
 * Thread-safe in-memory binary search tree implementation using
 * fine-grained locking (lock coupling).
 * <p>
 * Each {@link Node} maintains its own {@link ReentrantReadWriteLock},
 * allowing concurrent reads and localized writes.
 * <p>
 * The {@code put()} and {@code get()} methods use read locks during
 * traversal and upgrade to write locks only when modifying the tree.
 */
public class BTreeLocks implements MyBTree {
    private Node root;

    public BTreeLocks() {
        root = null;
    }


    /**
     * Inserts or updates a key-value pair in the tree.
     * <p>
     * Uses lock coupling: threads traverse nodes using read locks and
     * acquire write locks only when inserting into a leaf.
     *
     * @param key   the key as a byte array
     * @param value the value as a byte array
     */
    public void put(byte[] key, byte[] value) {
        Node newNode = new Node(key, value);

        if (root == null) {  // check root in cache (Lazy initialization)
            synchronized (this) {
                if (root == null) { //if there's no root then assign it
                    root = newNode;
                    return;
                }
            }
        }

        Node current = root;
        current.lock.readLock().lock(); // acquire read lock for traversal

        while (true) {
            int compareResult = current.compareTo(newNode);

            /* Update the value. There is no need for a write lock, since we are working
            with the value of the node and not the structure of the tree.
             */
            if (compareResult == 0) {
                current.lock.readLock().unlock();
                current.value = newNode.value;
                return;
            }

            Node next = compareResult > 0 ? current.leftChild : current.rightChild;

            if (next == null) {
                current.lock.readLock().unlock(); // Lock upgrading from read Lock
                current.lock.writeLock().lock();  // to writeLock

                // recalculation for safety to omit race condition in lock upgrading
                compareResult = current.compareTo(newNode);
                next = compareResult > 0 ? current.leftChild : current.rightChild;
                if (next == null) { // If nobody else changed next -> perform insertion

                    if (compareResult > 0) {
                        current.leftChild = newNode;
                    } else {
                        current.rightChild = newNode;
                    }

                    current.lock.writeLock().unlock();
                    return;
                }else{ // release locks and try again
                    current.lock.writeLock().unlock();
                    current.lock.readLock().lock();
                }
            } else { // Release previous lock and Acquire for the next Node
                next.lock.readLock().lock();
                current.lock.readLock().unlock();
                current = next;
            }
        }
    }

    /**
     * Retrieves a value by key using read locks only.
     * <p>
     * Traverses the tree under read locks and returns {@code null}
     * if the key is not found.
     *
     * @param key the key as a byte array
     * @return the associated value, or {@code null} if not found
     */
    public byte[] get(byte[] key) {
        Node current = root;
        if (current == null) return null;

        current.lock.readLock().lock();

        while (true) {
            // Found key and return value
            if (Util.compareByteArrays(current.key, key) == 0) {
                current.lock.readLock().unlock();
                return current.getValue();
            }

            // Move to the next node
            Node next = (Util.compareByteArrays(current.key, key) > 0) ? current.leftChild : current.rightChild;
            if (next == null) return null;

            next.lock.readLock().lock();
            current.lock.readLock().unlock();
            current = next;
        }
    }


    @Getter
    @Setter
    private static class Node implements Comparable<Node>, Comparator<Node> {
        private byte[] key;
        private byte[] value;
        private Node leftChild, rightChild;
        private final ReentrantReadWriteLock lock = new ReentrantReadWriteLock(true);

        public Node(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
        }

        public Node() {
        }

        @Override
        public int compareTo(Node o) {
            return Util.compareByteArrays(this.key, o.key);
        }

        @Override
        public int compare(Node o1, Node o2) {
            return Util.compareByteArrays(o1.key, o2.key);
        }
    }
}
