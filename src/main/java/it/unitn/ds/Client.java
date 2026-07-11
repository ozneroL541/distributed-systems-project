package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.*;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractClient {
    /**
     * List of all alive replicas of the system.
     * Integer is the id of the replica,
     * ActorRef is the reference of the Replica inside AKKA
     */
    private Map<Integer, ActorRef> replicas;
    /** HashMap to store Cancellable for timeout on the writeRequest message send to the coordinator */
    private final HashMap<ActorRef, Queue<Cancellable>> sendWriteRequestTimeouts = new HashMap<>();
    /** HashMap to store Cancellable for timeout on the writeRequest message send to the coordinator */
    private final HashMap<ActorRef, Queue<Cancellable>> sendReadRequestTimeouts = new HashMap<>();

    Client(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, Optional<ActorRef> listener) {
        super(readTimeoutDelay, writeTimeoutDelay, listener, defaultTargetReplica);
    }

    public static Props props(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.empty()));
    }

    // Props method for automated tests
    public static Props propsWithListener(long readTimeoutDelay, long writeTimeoutDelay, Optional<ActorRef> defaultTargetReplica, ActorRef listener) {
        return Props.create(Client.class, () -> new Client(readTimeoutDelay, writeTimeoutDelay, defaultTargetReplica, Optional.ofNullable(listener)));
    }

    // =============================


    // =================================================================================
    // Helper functions
    // =================================================================================
    /**
     * Helper function to schedule a timeout
     * @param time the time to wait before sending the timeout message (in milliseconds)
     * @param timeOut the timeOut message to schedule
     * @return A cancellable reference to the timeout that can be used to cancel the timeout if needed
     */
    Cancellable setTimeout(long time, TimeOut timeOut) {
        return getContext().system().scheduler().scheduleOnce(
                Duration.create(time, TimeUnit.MILLISECONDS),
                getSelf(),
                timeOut,
                getContext().system().dispatcher(), getSelf()
        );
    }

    public void initSystem(AbstractReplica.InitSystem sysInit) {
        this.replicas = sysInit.group;
    }


    @Override
    public void sendRead(ActorRef replica, int index) {
        log("Sending a Read request to:"+ replica.path().name());
        replica.tell(new AbstractClient.ReadRequest(index, replica), this.getSelf());
        this.sendReadRequestTimeouts
                .computeIfAbsent(replica, k -> new ArrayDeque<>())
                .add(setTimeout(this.getReadTimeoutDelay(),new TimeOut(TimeOut.TimeoutType.SendRead))); // TODO how much time to wait for coordinator?

        // TODO: implement
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        log("Sending a Write request to: " + replica.path().name() +" with content: {index:"+index+", value:"+value+"}");
        replica.tell(new AbstractClient.WriteRequest(index, value, replica),this.getSelf());
        this.sendWriteRequestTimeouts
                .computeIfAbsent(replica, k -> new ArrayDeque<>())
                .add(setTimeout(this.getWriteTimeoutDelay(),new TimeOut(TimeOut.TimeoutType.SendWrite))); // TODO how much time to wait for coordinator?
        // TODO: implement
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // onWriteRequest already matched by the abstract class
                .match(AbstractClient.ReadResult.class,  this::onReadResult)
                .match(AbstractClient.WriteResult.class, this::onWriteResult)
                .match(TimeOut.class,                    this::onTimeOut)
                .match(AbstractReplica.InitSystem.class, this::initSystem)
                // TODO add your message handlers here .match(, )
                .build();
    }

    private void onReadResult(AbstractClient.ReadResult msg) {
        ActorRef key = replicas.get(msg.fromReplica);
        Queue<Cancellable> timeouts = this.sendReadRequestTimeouts.get(key);
        if ( timeouts != null) {
            timeouts.poll().cancel();
            if (this.sendReadRequestTimeouts.get(key).isEmpty()) {
                this.sendReadRequestTimeouts.remove(key);
            }
        }
        callbackOnReadResult(msg);
    }

    private void onWriteResult(AbstractClient.WriteResult msg) {
        ActorRef key = this.replicas.get(msg.fromReplica);
        Queue<Cancellable> timeouts = this.sendWriteRequestTimeouts.get(key);
        if ( timeouts != null) {
            timeouts.poll().cancel();
            if (this.sendWriteRequestTimeouts.get(key).isEmpty()) {
                this.sendWriteRequestTimeouts.remove(key);
            }
        }
        callbackOnWriteResult(msg);
    }

    /**
     * Handle a timeout message
     * @param msg the timeout message
     */
    private void onTimeOut(TimeOut msg) {
        switch (msg.type) {
            case TimeOut.TimeoutType.SendRead:
                log("Timeout on read request");
                break;
            case TimeOut.TimeoutType.SendWrite:
                log("Timeout on write request");
                break;
            case TimeOut.TimeoutType.UpdateRequest:
                log("Timeout on update request");
                break;
            case TimeOut.TimeoutType.WriteRequest:
                log("Timeout on write request");
                break;
            case TimeOut.TimeoutType.Heartbeat:
                log("Timeout on heartbeat");
                break;
            case TimeOut.TimeoutType.Election:
                log("Timeout on election");
                break;
            default:
                break;
        }
    }

}
