package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractClient {
    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }
    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    /**
     * Helper function to extract the replica ID from the ActorRef of the replica.
     * The ActorRef name is expected to be in the format "Replica_<ID>".
     * If the format is not as expected, it returns the full name of the ActorRef.
     * @param replica The ActorRef of the replica from which to extract the ID.
     * @return The extracted replica ID as a String, or the full name of the ActorRef if extraction fails.
     */
    private static String getReplicaID(ActorRef replica) {
        /** Full replica name */
        final String replicaName = replica.path().name();
        try {
            return replicaName.split("_")[1];
        } catch (Exception e) {
            return replicaName; // Fallback to the full name if extraction fails
        }
    }

    /**
     * List of all alive replicas of the system.
     * Integer is the id of the replica,
     * ActorRef is the reference of the Replica inside AKKA
     */
    /** HashMap to store Cancellable for timeout on the writeRequest message send to the coordinator */
    private final HashMap<String, Queue<Cancellable>> sendWriteRequestTimeouts = new HashMap<>();

    /** HashMap to store Cancellable for timeout on the writeRequest message send to the coordinator */
    private final HashMap<String, Queue<Cancellable>> sendReadRequestTimeouts = new HashMap<>();

    // =============================


    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    // =================================================================================
    // Helper functions
    // =================================================================================
    @Override
    public void sendRead(ActorRef replica, int index) {
        // TODO: Should be ReplicaID
        // – [Client <ClientName>] requesting READ (<idx>) to <ReplicaID>
        log("requesting READ " + index + " to " + Client.getReplicaID(replica));
        replica.tell(new AbstractClient.ReadRequest(index, replica), this.getSelf());
        Queue<Cancellable> queue = this.sendReadRequestTimeouts.computeIfAbsent(replica.path().name(), k -> new ArrayDeque<>());
        queue.add(getContext().system().scheduler().scheduleOnce(
                Duration.create(this.getReadTimeoutDelay(),TimeUnit.MILLISECONDS),
                getSelf(),
                new AbstractClient.ReadTimeout(this.getSelf(),replica,index),
                getContext().system().dispatcher(), getSelf()
        ));
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        // TODO: Should be ReplicaID
        // – [Client <ClientName>] requesting WRITE (<idx>, <val>) to <ReplicaID>
        log("requesting WRITE " + index + ", " + value + " to " + Client.getReplicaID(replica));
        replica.tell(new AbstractClient.WriteRequest(index, value, replica),this.getSelf());
        Queue<Cancellable> queue = this.sendWriteRequestTimeouts.computeIfAbsent(replica.path().name(), k -> new ArrayDeque<>());
        queue.add(getContext().system().scheduler().scheduleOnce(
                Duration.create(this.getWriteTimeoutDelay(),TimeUnit.MILLISECONDS),
                getSelf(),
                new AbstractClient.WriteTimeout(this.getSelf(),replica,index, value),
                getContext().system().dispatcher(), getSelf()
        ));
    }
    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(Replica.ClientACK.class,  this::onResult)
                .match(AbstractClient.WriteTimeout.class, this::onWriteTimeOut)
                .match(AbstractClient.ReadTimeout.class,  this::onReadTimeOut)
                .build();
    }

    private void onResult(Replica.ClientACK msg) {
        String key = msg.actorRef.path().name();
        // Cancellable timeout;
        if (msg.msg instanceof WriteResult) {
            callbackOnWriteResult((WriteResult) msg.msg);
            this.sendWriteRequestTimeouts.get(key).remove().cancel();
            if (this.sendWriteRequestTimeouts.get(key).isEmpty()) {
                this.sendWriteRequestTimeouts.remove(key);
            }
        } else if (msg.msg instanceof  ReadResult) {
            callbackOnReadResult((ReadResult) msg.msg);
            this.sendReadRequestTimeouts.get(key).remove().cancel();
            if (this.sendReadRequestTimeouts.get(key).isEmpty()){
                this.sendReadRequestTimeouts.remove(key);
            }

        }
    }
    private void onWriteTimeOut(AbstractClient.WriteTimeout msg) {
        callbackOnWriteTimeout(msg);
        String key = msg.replica.path().name();
        this.sendWriteRequestTimeouts.get(key).remove().cancel();
        if (this.sendWriteRequestTimeouts.get(key).isEmpty()) {
            this.sendWriteRequestTimeouts.remove(key);
        }

    }

    private void onReadTimeOut(AbstractClient.ReadTimeout msg) {
        callbackOnReadTimeout(msg);
        String key = msg.replica.path().name();
        this.sendReadRequestTimeouts.get(key).remove().cancel();
        if (this.sendReadRequestTimeouts.get(key).isEmpty()){
            this.sendReadRequestTimeouts.remove(key);
        }
    }
}
