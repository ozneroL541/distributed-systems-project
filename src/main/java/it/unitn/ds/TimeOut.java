package it.unitn.ds;

import java.io.Serializable;

public class TimeOut implements Serializable {
    /**
     * TimeoutType
     */
    public enum TimeoutType {
        /** Client read */
        SendRead,
        /** Client write */
        SendWrite,
        /** If the WRITEOK message is not received on time after the UPDATE */
        UpdateRequest,
        /** If the UPDATE message is not received on time after
        forwarding an update REQUEST to the coordinator */
        WriteRequest,
        /** Coordinator heartbeat */
        Heartbeat,
        /** Election timeout */
        Election
    }
    /** The type of timeout */
    public final TimeoutType type;
    //public Serializable msg;
    //public String replica_name;
    /**
     * Constructor for TimeOut
     * @param type the type of timeout
     */
    public TimeOut(TimeoutType type) {
        this.type = type;
    }
    //public TimeOut(TimeoutType type, Serializable msg, String replica_name) {
    //    this.type = type;
    //    this.msg = msg;
    //    this.replica_name = replica_name;
    //}
}
