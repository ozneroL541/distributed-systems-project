package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;

class UpdateClock {
    /** Epoch number */
    private int e;
    /** Sequence number */
    private int i;
    /** History of updates */
    private final Map<Integer, Integer> history = new HashMap<>();
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
    public void incrementE() {
        this.e++;
        this.i = 0;
        this.history.put(this.e, this.i);
    }
    /**
     * Increment the sequence number
     */
    public void incrementI() {
        this.i++;
        this.history.put(this.e, this.i);
    }
}
