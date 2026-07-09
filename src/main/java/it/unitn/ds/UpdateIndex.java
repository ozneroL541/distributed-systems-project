package it.unitn.ds;

public class UpdateIndex {
    /** Epoch number */
    private int e;
    /** Sequence number */
    private int i;


    public UpdateIndex(int e, int i) {
        this.e = e;
        this.i = i;
    }
    public UpdateIndex() {
        this.e = 0;
        this.i = 0;
    }
    /**
     * Increment the epoch number
     */
    public void incrementE() {
        this.e++;
        this.i = 0;
    }
    /**
     * Increment the sequence number
     */
    public void incrementI() {
        this.i++;
    }
}
