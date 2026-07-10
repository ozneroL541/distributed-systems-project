package it.unitn.ds;

import java.io.Serializable;
import it.unitn.ds.AbstractReplica.Crash;

public class TimeOut implements Serializable {
    /** The type of timeout */
    public final Crash.Type type;
    /**
     * Constructor for TimeOut
     * @param type the type of timeout
     */
    public TimeOut(Crash.Type type) {
        this.type = type;
    }
}
