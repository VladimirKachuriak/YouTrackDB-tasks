package ch.unibe.task1;

import lombok.Getter;
import lombok.Setter;

import java.util.Comparator;
import java.util.concurrent.atomic.AtomicReference;

/**
 * Thread-safe in-memory binary search tree implementation using
 * CAS operation (Lock-Free).
 * <p>
 * Each {@link BTreeLocks.Node} has  {@link AtomicReference} to child Nodes.
 *  This allows CAS operations to be used for insertions.
 * <p>
 */
public class BTreeLockFree implements MyBTree {
    private Node root;

    public BTreeLockFree() {
        root = null;
    }

    /**
     * Inserts or updates a key-value pair in the tree.
     * <p>
     * This method uses a CAS to insert nodes atomically.
     * </p>
     *
     * @param key   the key as a byte array
     * @param value the value as a byte array
     */
    public void put(byte[] key, byte[] value) {
        Node newNode = new Node(key, value);

        if (root == null) { // Lazy Initialization
            synchronized (this) {
                if (root == null) {
                    root = newNode;
                    return;
                }
            }
        }

        Node current = root;
        while (true) {
            int compareResult = Util.compareByteArrays(current.key, key);

            // Update the value. There is no need for a write lock, since we are working with the value of the node
            // and not the structure of the tree.
            if (compareResult == 0) {
                current.value = newNode.value;
                return;
            }

            AtomicReference<Node> next = (compareResult > 0) ?
                    current.leftChild : current.rightChild;


            Node child = next.get();

            // Perform CAS to insert Node
            if (child == null) {
                if (compareResult > 0) {
                    if (next.compareAndSet(null, newNode)) return;
                    else continue;
                } else {
                    if (next.compareAndSet(null, newNode)) return;
                    else continue;
                }
            }
            current = child;
        }
    }

    /**
     * Retrieves a value by key using a non-blocking traversal.
     *
     * @param key the key as a byte array
     * @return the associated value, or {@code null} if not found
     */
    public byte[] get(byte[] key) {
        Node current = root;
        if (current == null) return null;

        while (true) {
            int compare = Util.compareByteArrays(current.key, key);

            // Traversing Tree
            if (compare == 0) {
                return current.getValue();
            } else if (compare > 0) {
                current = current.leftChild.get();
            } else {
                current = current.rightChild.get();
            }
        }
    }


    @Getter
    @Setter
    private static class Node implements Comparable<Node>, Comparator<Node> {
        private byte[] key;
        private byte[] value;
        private AtomicReference<Node> leftChild, rightChild;

        public Node(byte[] key, byte[] value) {
            this.key = key;
            this.value = value;
            leftChild = new AtomicReference<>();
            rightChild = new AtomicReference<>();
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
