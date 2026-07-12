package it.unitn.ds;

import java.io.Serializable;

/**
 * UpdateClock
 * Logical clock used to order updates in the system. It consists of an epoch number and a sequence number.
 * The epoch number is incremented when a new coordinator is elected, and the sequence number is incremented for each update made by the coordinator.
 * The clock can be synchronized with another clock to ensure that updates are applied in the correct order across replicas.
 */
public class UpdateClock implements Comparable<UpdateClock>, Serializable, Cloneable {
    /** Epoch number */
    private int e;
    /** Sequence number */
    private int i;
    /**
     * Constructor for UpdateClock
     * @param e the epoch number
     * @param i the sequence number
     */
    public UpdateClock(int e, int i) {
        this.e = e;
        this.i = i;
    }
    /**
     * Default constructor for UpdateClock
     */
    public UpdateClock() {
        this.e = 0;
        this.i = 0;
    }
    /**
     * Get the epoch number
     * @return the epoch number
     */
    public int getE() {
        return e;
    }
    /**
     * Get the sequence number
     * @return the sequence number
     */
    public int getI() {
        return i;
    }
    /**
     * Increment the epoch number
     */
    public synchronized void incrementE() {
        this.e++;
        this.i = 0;
    }
    /**
     * Increment the sequence number
     */
    public synchronized void incrementI() {
        this.i++;
    }
    @Override
    public int compareTo(UpdateClock arg0) {
        if (this.e != arg0.e) {
            return Integer.compare(this.e, arg0.e);
        }
        return Integer.compare(this.i, arg0.i);
    }
    /**
     * Synchronize the clock with a more recent clock
     * @param other the other clock to synchronize with
     */
    public synchronized void syncClock(UpdateClock other) {
        if (this.compareTo(other) < 0) {
            this.e = other.e;
            this.i = other.i;
        }
    }

    @Override
    public boolean equals(Object o) {
        if (!(o instanceof UpdateClock)) return false;
        return this.compareTo((UpdateClock) o) == 0;
    }

    @Override
    protected UpdateClock clone() {
        return new UpdateClock(this.e, this.i);
    }

//    @Override
//    public int hashCode() {
//        return Objects.hash(e, i);
//    }
}
