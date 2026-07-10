package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Props;

import java.util.Optional;

public class Client extends AbstractClient {

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

    @Override
    public void sendRead(ActorRef replica, int index) {
        log("Sending a Read request to:"+ replica.path().name());
        replica.tell(new AbstractClient.ReadRequest(index, this.getSelf()), this.getSelf());
        // TODO: implement        
    }

    @Override
    public void sendWrite(ActorRef replica, int index, int value) {
        log("Sending a Write request to: " + replica.path().name() +" with content: {index:"+index+", value:"+value+"}");
        replica.tell(new AbstractClient.WriteRequest(index,value, this.getSelf()),this.getSelf());
        // TODO: implement
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                // onWriteRequest already matched by the abstract class
                .match(AbstractClient.ReadResult.class,   this::callbackOnReadResult)
                .match(AbstractClient.WriteResult.class, this::callbackOnWriteResult)
                // TODO add your message handlers here .match(, )
                .build();
    }

}
