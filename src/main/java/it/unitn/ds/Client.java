package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import scala.concurrent.duration.Duration;

import java.util.ArrayDeque;
import java.util.HashMap;
import java.util.Optional;
import java.util.Queue;
import java.util.concurrent.TimeUnit;

public class Client extends AbstractClient {
    /** HashMap to store Cancellable for timeout on the writeRequest message send to the coordinator */
    private final HashMap<WriteResult, Queue<Cancellable>> sendWriteRequestTimeouts = new HashMap<>();
    /** HashMap to store Cancellable for timeout on the writeRequest message send to the coordinator */
    private final HashMap<ReadRequest, Queue<Cancellable>> sendReadRequestTimeouts = new HashMap<>();

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


    @Override
    public void sendRead(ActorRef replica, int index) {
        log("Sending a Read request to:"+ replica.path().name());
        replica.tell(new AbstractClient.ReadRequest(index), this.getSelf());
        this.sendReadRequestTimeouts
                .computeIfAbsent(new ReadRequest(index, replica), k -> new ArrayDeque<>())
                .add(setTimeout(this.getReadTimeoutDelay(),new TimeOut(TimeOut.TimeoutType.SendRead))); // TODO how much time to wait for coordinator?

        // TODO: implement
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        log("Sending a Write request to: " + replica.path().name() +" with content: {index:"+index+", value:"+value+"}");
        replica.tell(new AbstractClient.WriteRequest(index,value, this.getSelf()),this.getSelf());
        this.sendWriteRequestTimeouts
                .computeIfAbsent(2, k -> new ArrayDeque<>())
                .add(setTimeout(this.getWriteTimeoutDelay(),new TimeOut(TimeOut.TimeoutType.SendWrite))); // TODO how much time to wait for coordinator?
        // TODO: implement
        for (Integer name: sendWriteRequestTimeouts.keySet()) {
            int key = name;
            String coso = sendWriteRequestTimeouts.get(name).toString();
            debug(key + " " + coso);
        }
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // onWriteRequest already matched by the abstract class
                .match(AbstractClient.ReadResult.class,   this::onReadResult)
                .match(AbstractClient.WriteResult.class, this::onWriteResult)
                .match(TimeOut.class,                    this::onTimeOut)
                // TODO add your message handlers here .match(, )
                .build();
    }

    private void onReadResult(AbstractClient.ReadResult msg) {
        ReadRequest key = new ReadRequest(msg.index);
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
//        WriteRequest key = new WriteRequest(msg.index, msg.value);
        Integer key = 2;
        Queue<Cancellable> timeouts = this.sendWriteRequestTimeouts.get(key);
        debug(timeouts.toString());
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
