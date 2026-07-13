package it.unitn.ds;

import akka.actor.ActorRef;
import akka.actor.Cancellable;
import akka.actor.Props;
import it.unitn.ds.AbstractClient.WriteRequest;
import it.unitn.ds.TimeOut.TimeoutType;
import scala.concurrent.duration.Duration;

import java.io.Serializable;
import java.util.*;
import java.util.concurrent.TimeUnit;

/**
 * Replica
 */
public class Replica extends AbstractReplica {
    // =================================================================================
    // Messages classes
    // =================================================================================
    public static class UpdateRequest implements Serializable {
        UpdateClock identifier;
        ActorRef coordinator;
        AbstractClient.WriteRequest writeRequest;

        public UpdateRequest(UpdateClock identifier, ActorRef coordinator, AbstractClient.WriteRequest writeRequest) {
            this.identifier = identifier;
            this.coordinator = coordinator;
            this.writeRequest = writeRequest;
        }
    }
    public static class UpdateACK implements Serializable {
        UpdateClock identifier;
        public UpdateACK(UpdateClock identifier){
            this.identifier = identifier;
        }
    }
    public static class WriteOK implements Serializable {
        UpdateClock identifier;
        public WriteOK(UpdateClock identifier){
            this.identifier = identifier;
        }
    }
    public static class ClientACK implements Serializable {
        ActorRef actorRef;
        Serializable msg;

        public ClientACK(ActorRef actorRef, Serializable msg) {
            this.actorRef = actorRef;
            this.msg = msg;

        }
    }
    public static class Synchronization implements Serializable {
        /** The missing history of updates */
        final Map<UpdateClock, WriteRequest>  missingHistory;
        /**
         * Constructor for Synchronization
         * @param missingHistory the missing history of updates
         */
        public Synchronization(Map<UpdateClock, WriteRequest> missingHistory) {
            this.missingHistory = new TreeMap<>(missingHistory);
        }
        /**
         * Constructor for Synchronization
         * @param history the whole history of updates
         * @param worstClock the worst clock value among the replicas
         */
        public Synchronization(Map<UpdateClock, WriteRequest> history, UpdateClock worstClock) {
            this.missingHistory = new TreeMap<>(Replica.shortHistory(history, worstClock));
        }
    }
    /**
     * ElectionMessage
     * This class represents a message used in the election process to determine the coordinator among replicas.
     */
    public static class ElectionMessage implements Serializable, Cloneable {
        /** The ID of the node that started the election */
        public final int electionStarter;
        /** A map of all candidates in the election, with their IDs as keys and clock values as values */
        public final Map<Integer, UpdateClock> candidates = new HashMap<Integer, UpdateClock>();
        /**
         * Constructor for ElectionMessage
         * @param electionStarter the ID of the node that started the election
         * @param nodeClock the clock value of the node that started the election
         */
        public ElectionMessage(int electionStarter, UpdateClock nodeClock) {
            final UpdateClock clock = nodeClock.clone();
            this.electionStarter = electionStarter;
            this.candidates.put(electionStarter, clock);
        }
        /**
         * Update the message with the information of a replica
         * @param replica the replica that updates the message
         */
        public ElectionMessage updateMsg(Replica replica) {
            final UpdateClock clock = replica.waitingForWriteOkUpdateClock.clone();
            this.candidates.put(replica.id, clock);
            this.deleteCrashedNodesFromCandidates(replica.replicas);
            return (ElectionMessage) this.clone();
        }
        /**
         * Clone the election message
         * @return a clone of the election message
         */
        public Object clone() {
            ElectionMessage clone = new ElectionMessage(this.electionStarter, this.candidates.get(this.electionStarter));
            for (Map.Entry<Integer, UpdateClock> entry : this.candidates.entrySet()) {
                clone.candidates.put(entry.getKey(), entry.getValue());
            }
            return clone;
        }
        /**
         * Get the best candidate for the election based on the highest clock and ID.
         * The best candidate is the one with the highest clock value,
         * and in case of a tie, the one with the highest ID.
         * @return the ID of the best candidate, or null if there are no candidates
         */
        public Integer getBestCandidate() {
            /** Best candidate */
            Integer bestCandidate = null;
            /** List of all candidates with the same best clock */
            List<Integer> candidatesList =  null;
            /** Best clock in the election */
            UpdateClock bestClock = candidates.values().stream().max(UpdateClock::compareTo).orElse(null);
            // If there is no best clock, return null
            if (bestClock == null) {
                return null;
            }
            // Get all candidates with the best clock
            candidatesList = candidates.entrySet().stream()
                    .filter(entry -> entry.getValue().equals(bestClock))
                    .map(Map.Entry::getKey)
                    .toList();
            if (candidatesList.isEmpty()) {
                return null;
            }
            // Get the candidate with the highest ID between the candidates with the best clock
            bestCandidate = candidatesList.stream().max(Integer::compareTo).orElse(null);
            return bestCandidate;
        }
        /**
         * Get the worst clock among the candidates in the election message.
         * @return the worst clock, or null if there are no candidates
         */
        public UpdateClock getWorstClock() {
            final UpdateClock worstClock = candidates.values().stream().min(UpdateClock::compareTo).orElse(new UpdateClock()).clone();
            return worstClock;
        }
        /**
         * Check if the election is over based on the election starter.
         * @param electionInitiator the ID of the election initiator
         * @return true if the election is over, false otherwise
         */
        public boolean isElectionOver(Integer electionInitiator) {
            if (electionInitiator == null) {
                return false;
            }
            return this.electionStarter == electionInitiator;
        }
        /**
         * Delete crashed nodes from the election message 
         * based on the current list of replicas.
         * Delete the id and the clock of the crashed nodes from the election message
         * where the id is not present in the list of alive replicas.
         * @param replicas list of alive replicas according to the current node
         */
        public void deleteCrashedNodesFromCandidates(Map<Integer, ActorRef> replicas) {
            if (replicas == null || replicas.isEmpty()) {
                return;
            }
            this.candidates.keySet().removeIf(id -> !replicas.containsKey(id));
        }
        /**
         * Delete crashed nodes from replicas list based on the current list of candidates.
         * @param replicas list of alive replicas according to the current node
         * @return the updated list of candidates after removing crashed nodes
         */
        public Map<Integer, ActorRef> deleteCrashedNodesFromList(Map<Integer, ActorRef> replicas) {
            if (replicas == null || replicas.isEmpty() || this.candidates == null || this.candidates.isEmpty()) {
                return replicas;
            }
            replicas.keySet().removeIf(id -> !this.candidates.containsKey(id));
            return replicas;
        }
    }
    /**
     * MessageWithACK
     */
    public static abstract class MessageWithACK implements Serializable {
        /** The ID of the sender of the message */
        public final int senderId;
        /** The message to be acknowledged */
        public final Serializable msg;

        public MessageWithACK(int senderId, Serializable msg) {
            this.senderId = senderId;
            this.msg = msg;
        }
        /**
         * Get the acknowledgment for the message
         * @return the acknowledgment message
         */
        public abstract Serializable getACK();
        /**
         * Update the sender ID of the message
         * @param senderId the new sender ID
         * @return a new instance of the message with the updated sender ID
         */
        public abstract MessageWithACK updateSender(int senderId);
    }
    /**
     * Election Message with Acknowledgment
     * This class represents an election message that requires an acknowledgment from the receiver.
     */
    public static class Election extends MessageWithACK {
        /**
         * Constructor for Election
         * @param electionStarter the ID of the node that started the election
         * @param nodeClock the clock value of the node that started the election
         */
        public Election(int electionStarter, UpdateClock nodeClock) {
            super(electionStarter, new ElectionMessage(electionStarter, nodeClock));
        }
        public Election(int senderID, ElectionMessage msg) {
            super(senderID, msg);
        }
        @Override
        public Serializable getACK() {
            return this.getACKMsg();
        }
        /**
         * Update the election message with the information of a replica
         * @param replica the replica that updates the election message
         */
        public Election updateMsg(Replica replica) {
            // Update the message with the information of the replica
            if (this.msg instanceof ElectionMessage) {
                ElectionMessage electionMsg = (ElectionMessage) this.msg;
                ElectionMessage updatedMsg = electionMsg.updateMsg(replica);
                return new Election(this.senderId, updatedMsg);
            }
            return this;
        }
        /**
         * Get the ElectionMessage from the Election message.
         * @return the ElectionMessage, or null if the message is not of type ElectionMessage
         */
        public ElectionMessage getMsg() {
            if (this.msg instanceof ElectionMessage) {
                return (ElectionMessage) this.msg;
            }
            return null;
        }
        /**
         * Get the acknowledgment for the election message.
         * @return the acknowledgment message
         */
        public ElectionAck getACKMsg() {
            return new ElectionAck(this.msg);
        }
        @Override
        public Election updateSender(int senderId) {
            return new Election(senderId, (ElectionMessage) this.msg);
        }
    }
    /**
     * ElectionChooseCoordinator
     */
    public static class ElectionChooseCoordinator implements Serializable {
        /** The elected coordinator */
        public final CoordinatorElected coordinatorElected;
        /** The worst clock value among the replicas */
        public final UpdateClock worstClock;
        /**
         * Constructor for ElectionChooseCoordinator
         * @param coordinatorElected the elected coordinator
         * @param worstClock the worst clock value among the replicas
         */
        public ElectionChooseCoordinator(CoordinatorElected coordinatorElected, UpdateClock worstClock) {
            this.coordinatorElected = new CoordinatorElected(coordinatorElected.newCoordinatorId, coordinatorElected.replicaId);
            this.worstClock = worstClock.clone();
        }
    }
    /**
     * ElectionOver
     * This class represents a message indicating that the election process is over and a new coordinator has been elected.
     */
    public static class ElectionOver extends MessageWithACK {
        /**
         * Constructor for ElectionOver
         * @param senderId the ID of the sender of the message
         * @param msg the message to be acknowledged
         */
        public ElectionOver(int senderId, ElectionChooseCoordinator msg) {
            super(senderId, msg);
        }
        @Override
        public Serializable getACK() {
            return this.getACKMsg();
        }
        /**
         * Get the ElectionChooseCoordinator message from the ElectionOver message.
         * @return the ElectionChooseCoordinator message, or null if the message is not of type ElectionChooseCoordinator
         */
        public ElectionChooseCoordinator getMsg() {
            if (this.msg instanceof ElectionChooseCoordinator) {
                return (ElectionChooseCoordinator) this.msg;
            }
            return null;
        }
        /**
         * Get the acknowledgment for the election over message.
         * @return the acknowledgment message
         */
        public ElectionAck getACKMsg() {
            return new ElectionAck(this.msg);
        }
        @Override
        public ElectionOver updateSender(int senderId) {
            return new ElectionOver(senderId, (ElectionChooseCoordinator) this.msg);
        }
    }
    public static class ElectionAck implements Serializable {
        /** The election message that is being acknowledged */
        public final Serializable electionMessage;
        /**
         * Constructor for ElectionAck
         * @param electionMessage the election message that is being acknowledged
         */
        public ElectionAck(Serializable electionMessage) {
            this.electionMessage = electionMessage;
        }
    }
    /**
     * CoordinatorHeartbeat
     * This class represents a heartbeat message sent by the coordinator to indicate that it is alive.
     */
    public static class CoordinatorHeartbeat implements Serializable {
        /** The ID of the coordinator sending the heartbeat */
        public final int currentCoordinatorId;
        /** The timestamp of the heartbeat */
        public CoordinatorHeartbeat(int coordinatorId) {
            this.currentCoordinatorId = coordinatorId;
        }
    }
    /**
     * Record to represent a write request from a client
     * @param clientRef the reference of the client that sent the write request
     * @param writeRequest the write request sent by the client
     */
    private record ClientWrite(ActorRef clientRef, AbstractClient.WriteRequest writeRequest){ }
    public static Props props(int id, int minLatency, int maxLatency, int coordinatorBeatInterval) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.empty()));
    }
    // Props method for automated tests
    public static Props propsWithListener(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, ActorRef listener) {
        return Props.create(Replica.class, () -> new Replica(id, minLatency, maxLatency, coordinatorBeatInterval, Optional.ofNullable(listener)));
    }
    /**
     * Get the shortened history of updates that are more recent than the given clock.
     * @param history the history of updates to be shortened
     * @param clock the clock to start from to get the shortened history
     * @return the sorted history of updates that are more recent than the given clock
     */
    private static Map<UpdateClock, AbstractClient.WriteRequest> shortHistory(Map<UpdateClock, AbstractClient.WriteRequest> history, UpdateClock clock) {
        if (history == null || history.isEmpty()) {
            return history;
        }
        /** The shortened history of updates */
        final Map<UpdateClock, AbstractClient.WriteRequest> shortnedHistory = new TreeMap<UpdateClock, AbstractClient.WriteRequest>();
        for (Map.Entry<UpdateClock, AbstractClient.WriteRequest> entry : history.entrySet()) {
            if (entry.getKey().compareTo(clock) > 0) {
                shortnedHistory.put(entry.getKey(), entry.getValue());
            }
        }
        return shortnedHistory;
    }
    /** List of positions in a replica */
    private final int[] positions = new int[AbstractReplica.POSITIONS_LIST_LENGTH];
    /** The update clock for the replica */
    private final UpdateClock updateClock;
    /** The id of the coordinator */
    private int coordinatorID = 0;
    /** The number of replicas in the system */
    private int numberOfReplicas;
    /**
     * List of all alive replicas of the system.
     * Integer is the id of the replica,
     * ActorRef is the reference of the Replica inside AKKA
     */
    private Map<Integer, ActorRef> replicas;
    // ________________________________
    // Replica specific variables
    // ________________________________
    /** The update clock for the replica that track the last update put in waitingForWriteOK */
    private final UpdateClock waitingForWriteOkUpdateClock;
    /** scheduled crash type */
    private Crash.Type scheduled_crash_type;
    /** scheduled crash countdown */
    private int scheduled_crash_countdown;

    /** HashMap to store the history of update commited by the replica */
    private final Map<UpdateClock, AbstractClient.WriteRequest> history = new HashMap<>();

    /** HashMap to store the update ready to be commited by the replica but waiting for the WriteOK message*/
    private final Map<UpdateClock, AbstractClient.WriteRequest> waitingForWriteOK = new HashMap<>();

    /** Queue for pending write requests */
    private final Queue<ClientWrite> pendingWrites = new ArrayDeque<>();

    /** HashMap to store Cancellable for timeout on the writeRequest message send to the coordinator */
    private final HashMap<AbstractClient.WriteRequest, Queue<Cancellable>> writeRequestTimeouts = new HashMap<>();

    /** HashMap to store Cancellable for timeout on the election message send to the coordinator */
    private final HashMap<Serializable, Queue<Cancellable>> electionTimeouts = new HashMap<>();

    //private final HashMap<String, Queue<Cancellable>> StringelectionTimeouts = new HashMap<>();
    /** HashMap to store Cancellable for timeout on the updateRequest message send to the coordinator */
    private final HashMap<UpdateClock, Cancellable> updateRequestTimeouts = new HashMap<>();

    /** Cancellable for timeout on the coordinator heartbeat */
    private Cancellable coordinatorHeartbeatTimeout = null;


    /** Store the Sync message if you are still in an election and don't know the new leader */
    private Synchronization syncMessage = null;

    /** Store the UpdateRequest messages if you are still in an election and don't know the new leader */
    private Queue<UpdateRequest> updateRequestQueue = new ArrayDeque<>();

    /** Store the WriteOK messages if you are still in an election and don't know the new leader */
    private Queue<WriteOK> writeOKQueue = new ArrayDeque<>();

    // ________________________________
    // Coordinator specific variables
    // ________________________________
    /** Hashmap to count the number of ACK received for each message sent by the coordinator */
    private final Map<UpdateClock, Integer> UpdateACKCounter = new HashMap<>();
    /** The update clock for the next message to be sent */
    private final UpdateClock nextMessageClock;
    /** How long to wait for a timeout */
    private final long TIMEOUT_DELAY = this.getMaxLatencyPlusTolerance();

    /** 
     * Current election status, 
     * null if no election is in progress 
     * else the ID of the election starter
     */
    private Integer electionInProgress = null;

    /** The ID of the next replica in a ring topology */
    private Integer nextReplicaID = null;

    /**
     * Constructor for Replica
     * @param id the id of the replica
     */
    public Replica(int id) {
        this(id, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL, Optional.empty());
    }

    /**
     * Constructor for Replica
     * @param id the id of the replica
     * @param minLatency the minimum latency
     * @param maxLatency the maximum latency
     * @param coordinatorBeatInterval the interval for coordinator beats
     * @param listener the listener for the replica
     */
    public Replica(int id, int minLatency, int maxLatency, int coordinatorBeatInterval, Optional<ActorRef> listener) {
        super(id, minLatency, maxLatency, coordinatorBeatInterval, listener);
        this.updateClock = new UpdateClock();
        this.nextMessageClock = new UpdateClock();
        this.waitingForWriteOkUpdateClock = new UpdateClock();
    }

    @Override
    public int getSystemNumberOfActors() {
        //  return 0;
        return this.numberOfReplicas;
        //return this.replicas.size();
    }

    // =================================================================================
    // Messages handler functions
    // =================================================================================

    @Override
    public void crash(AbstractReplica.Crash how_to_crash) {
        if (how_to_crash.type == Crash.Type.Now || how_to_crash.after_n_messages_of_type == 0) {
            log("Replica crashed");
            getContext().become(crashed());
        } else {
            this.scheduled_crash_type = how_to_crash.type;
            this.scheduled_crash_countdown = how_to_crash.after_n_messages_of_type;
        }

    }

    @Override
    public void initSystem(InitSystem sysInit) {
        this.replicas = new HashMap<>(sysInit.group);
        this.numberOfReplicas = sysInit.group.size();
        int coordinator_id = sysInit.coordinator_id;
        this.coordinatorID = coordinator_id;
        log("I set as coordinator: "+coordinator_id);
        if (this.isCoordinator()) {
            this.sendHeartbeat();
        }
    }

    @Override
    public final Receive createReceive() {
        return createBaseReceiveBuilder()
                .match(Crash.class, this::crash)
                .match(AbstractClient.ReadRequest.class,  this::onReadRequest)
                .match(AbstractClient.WriteRequest.class, this::onWriteRequest)
                .match(Replica.UpdateRequest.class,       this::onUpdateRequest)
                .match(Replica.UpdateACK.class,           this::onUpdateACK)
                .match(Replica.WriteOK.class,             this::onWriteOK)
                .match(TimeOut.class,                     this::onTimeOut)
                .match(Election.class,                    this::onElectionMessage)
                .match(ElectionOver.class,                this::onElectionOver)
                .match(ElectionAck.class,                 this::onElectionAck)
                .match(CoordinatorHeartbeat.class,        this::onHeartbeat)
                .build();
    }

    public final Receive createElectionReceive() {
        return createBaseReceiveBuilder()
                .match(AbstractClient.ReadRequest.class,  this::onReadRequest)
                .match(AbstractClient.WriteRequest.class, w -> this.pendingWrites.add(new ClientWrite(getSender(), w)))
                .match(TimeOut.class,                     this::onTimeOut)
                .match(Election.class,                    this::onElectionMessage)
                .match(ElectionOver.class,                this::onElectionOver)
                .match(ElectionAck.class,                 this::onElectionAck)
                .match(Synchronization.class,             this::onSyncMessage)
                .match(CoordinatorHeartbeat.class,        this::onHeartbeat)
                .match(Replica.UpdateRequest.class,       u -> this.updateRequestQueue.add(u))
                .match(Replica.WriteOK.class,       wOK -> this.writeOKQueue.add(wOK))
                .build();
    }

    public Receive crashed() {
        return receiveBuilder()
                .matchAny(msg -> {})
                .build();
    }

    /**
     * Check if this replica is the coordinator.
     * @return true if this replica is the coordinator, false otherwise
     */
    public boolean isCoordinator() {
        return this.coordinatorID == this.id;
    }
    /**
     * Check if an election is in progress.
     * @return true if an election is in progress, false otherwise
     */
    public boolean isElectionInProgress() {
        return this.electionInProgress != null;
    }
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
    /**
     * Multicast a message to all the replicas
     * @param m the message to multicast
     */
    void multicast(Serializable m, Crash.Type crash_type) {
        // Send the message to all replicas
        for (ActorRef r : replicas.values()) {
            // Send the message to the replica
            r.tell(m, this.getSelf());
            // Check if this replica is scheduled to crash after sending a message of this type
            if (crash_type != null && this.checkIfTimeToCrash(crash_type)) {
                return;
            }
        }
    }
    private boolean checkIfTimeToCrash(Crash.Type type) {
        if (this.scheduled_crash_type == type) {
            this.scheduled_crash_countdown-=1;
            if (this.scheduled_crash_countdown <= 0) {
                crash(new Crash(Crash.Type.Now, 0));
                return true;
            }
        }
        return false;
    }
    private void onWriteRequest(AbstractClient.WriteRequest msg) {
        if (getSender() != msg.replica) {
            debug("Inserting the request inside my pending write queue");
            this.pendingWrites.add(new ClientWrite(getSender(), msg));
        }
        log("Received a Write request by "+ getSender().path().name() + " with content: {index:"+msg.index+", value:"+msg.value+"}");
        if (this.isCoordinator()) {
            log("I'm the coordinator; Sending the update message");
            this.nextMessageClock.incrementI();
            UpdateClock clock = new UpdateClock(this.nextMessageClock.getE(), this.nextMessageClock.getI());
            UpdateRequest updateRequest = new UpdateRequest(clock, this.getSelf(), msg);
            debug("Sending message with "+ updateRequest.identifier.getE()+updateRequest.identifier.getI());
            this.UpdateACKCounter.put(updateRequest.identifier, 0);
            multicast(updateRequest, Crash.Type.Update);

        }
        else {
            log("Sending an update request to the coordinator (ID: " + this.coordinatorID + ")" + " with content: {index:" + msg.index + ", value:" + msg.value + "}");
            ActorRef coordinator = this.replicas.get(this.coordinatorID);
            coordinator.tell(msg, this.getSelf());
            this.writeRequestTimeouts.computeIfAbsent(msg, k -> new ArrayDeque<>())
                    .add(setTimeout(TIMEOUT_DELAY,new TimeOut(TimeOut.TimeoutType.WriteRequest)));
        }
    }
    private void onUpdateRequest(Replica.UpdateRequest msg) {
        log("Received an Update request from coordinator with content: {index:"+msg.writeRequest.index+", value:"+msg.writeRequest.value+"} and id: "+ msg.identifier.getE()+":"+msg.identifier.getI());
        // cancel the WriteRequest timeout
        Queue<Cancellable> timeouts = this.writeRequestTimeouts.get(msg.writeRequest);
        if ( timeouts != null) {
            timeouts.poll().cancel();
            if (this.writeRequestTimeouts.get(msg.writeRequest).isEmpty()) {
                this.writeRequestTimeouts.remove(msg.writeRequest);
            }
        }
        // add update to waiting for WriteOK
        this.waitingForWriteOkUpdateClock.syncClock(msg.identifier);
        waitingForWriteOK.put(msg.identifier, msg.writeRequest);
        debug("Sending updateACK :" + msg.identifier.getI() +"---"+ msg.writeRequest.index +" "+msg.writeRequest.index);
        msg.coordinator.tell(new UpdateACK(msg.identifier), this.getSelf());
        this.updateRequestTimeouts
                .putIfAbsent(msg.identifier, setTimeout(TIMEOUT_DELAY,new TimeOut(TimeOut.TimeoutType.UpdateRequest)));
        // CRASH UPDATE
        this.checkIfTimeToCrash(Crash.Type.Update);
    }
    private void onUpdateACK(Replica.UpdateACK msg) {
        debug("received ack from "+ getSender().path().name()+"for clock: "+ msg.identifier.getE()+msg.identifier.getI());
        Integer ACKnumber = UpdateACKCounter.get(msg.identifier);
        if (ACKnumber == null) {
            debug("NULL????? "+ msg.identifier.getI() +" -- " +ACKnumber);
            return;
        }
        ACKnumber += 1;
        int quorum = replicas.size()/2 +1;
        if (ACKnumber >= quorum) {
            WriteOK writeOK = new WriteOK(msg.identifier);
            multicast(writeOK, Crash.Type.WriteOK);
            this.UpdateACKCounter.remove(msg.identifier);
        } else {
            UpdateACKCounter.put(
                    msg.identifier,
                    ACKnumber
            );
        }

    }
    private void onWriteOK(Replica.WriteOK msg) {
        log("Received a WriteOK message from the coordinator, applying the update");
        // cancel the UpdateRequest timeout
        Cancellable timeout = this.updateRequestTimeouts.remove(msg.identifier);
        if ( timeout != null) {
            timeout.cancel();

        }
        UpdateClock identifier = msg.identifier;
        // Check if the update clock of this replica is greater than the identifier of the WriteOK message
        if (this.updateClock.compareTo(identifier) > 0 ) {
            return;
        }
        AbstractClient.WriteRequest writeRequest = this.waitingForWriteOK.remove(identifier);
        this.positions[writeRequest.index] = writeRequest.value;
        this.history.put(identifier,writeRequest);
        this.updateClock.syncClock(identifier);
        this.callbackOnUpdateApplied(writeRequest.index,writeRequest.value);
        debug("New positions is: "+ Arrays.toString(this.positions));
        debug("pending write"+ pendingWrites.toString());

        getPendingWrite(writeRequest);
    }
    private void onReadRequest(AbstractClient.ReadRequest msg) {
        debug("Received a Read request from client");
        int position = this.positions[msg.index];
        getSender().tell( new Replica.ClientACK(this.getSelf(), new AbstractClient.ReadResult(true,msg.index, position, this.id)), this.getSelf());
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
            case TimeOut.TimeoutType.WriteRequest:
                debug("SIAMSO CRASHDHAHFHDSHFHDFHDHSAFHHDHFHHFHHFH");
                this.coordinatorCrashed();
                break;
            case TimeOut.TimeoutType.Heartbeat:
                debug("TIMEOUT on HeartBeat");
                this.onHeartbeatTimeout();
                break;
            case TimeOut.TimeoutType.Election:
                this.onElectionTimeout(msg);
                break;
            default:
                break;
        }
    }
    /**
     * Handle the event when the coordinator has crashed by removing it from the list of replicas and starting a new election if necessary.
     */
    private void coordinatorCrashed() {
        this.nodeCrashed(this.coordinatorID);
    }
    /**
     * Handle the event when the coordinator has crashed by starting a new election if one is not already in progress.
     */
    private void onCoordinatorCrash() {
        if (!this.isElectionInProgress()) {
            this.startElection();
        }
        // Change the receiver to handle election messages
        getContext().become(createElectionReceive());
        // Delete heartbeat timeout if it exists
        if (this.coordinatorHeartbeatTimeout != null) {
            this.coordinatorHeartbeatTimeout.cancel();
            this.coordinatorHeartbeatTimeout = null;
        }
    }
    /**
     * Handle the event when a new coordinator is elected.
     */
    private void onNewCoordinator() {
        // Callback to notify that a new coordinator has been elected
        this.callbackOnCoordinatorElected(this.coordinatorID);
        // Reset heartbeat timeout since a new coordinator has been elected
        this.coordinatorHeartbeatTimeout = this.setTimeout( COORDINATOR_BEAT_INTERVAL * TIMEOUT_DELAY, new TimeOut(TimeOut.TimeoutType.Heartbeat));
        // If this replica is the new coordinator, perform necessary actions
        if (this.isCoordinator()) {
            this.onBecameCoordinator();
        }
    }
    /**
     * Update the coordinator ID and handle the event when a new coordinator is elected.
     * @param newCoordinatorId the ID of the new coordinator
     */
    private void newCoordinator(int newCoordinatorId) {
        // If the new coordinator ID is the same as the current coordinator ID, do nothing
        if (this.coordinatorID == newCoordinatorId) {
            return;
        }
        // Update the coordinator ID
        this.coordinatorID = newCoordinatorId;
        // Handle the event when a new coordinator is elected
        this.onNewCoordinator();
    }
    /**
     * Handle the event when this replica becomes the coordinator.
     */
    private void onBecameCoordinator() {
        this.sendHeartbeat();
        // preare the Sync message
        this.updateHistory();
    }
    /**
     * Get the shortened history of updates that are more recent than the given clock in order by UpdateClock.
     * @param clock the clock to start from to get the shortened history
     * @return the sorted history of updates that are more recent than the given clock
     */
    private Map<UpdateClock, AbstractClient.WriteRequest> getShortnedHistory(UpdateClock clock) {
        return shortHistory(this.history, clock);
    }
    /**
     * Handle the event when this replica is elected as the new coordinator by sending a heartbeat and performing any necessary actions.
     * @param worstClock the worst clock value among the replicas
     */
    private void onElectedCoordinator(int newCoordinatorId, UpdateClock worstClock) {
        /** The shortened history of updates */
        final Map<UpdateClock, AbstractClient.WriteRequest> shortnedHistory = this.getShortnedHistory(worstClock);
        // Update the coordinator ID and handle the event when a new coordinator is elected
        this.newCoordinator(newCoordinatorId);
        // If this replica is the new coordinator, create a Synchronization message with the current history and worst clock
        if (this.isCoordinator()) {
            // Create a Synchronization message with the current history and worst clock
            Synchronization syncMessage = new Synchronization(shortnedHistory);
            // Go to next epoch
            this.updateClock.incrementE();
            // Update the next message clock to synchronize with the current update clock
            this.nextMessageClock.syncClock(this.updateClock);
            // send the Sync message to all replicas
            this.multicast(syncMessage, null);
        }
    }
    /**
     * Handle a coordinator elected message by updating the coordinator ID and
     * forwarding the message to the next replica if necessary.
     * @param msg the coordinator elected message
     */
    private void onElectionOver(ElectionOver msg) {
        debug("ELECTION IS OVER, I RECEIVE " + msg.toString());
        // Update the coordinator ID and handle the event when a new coordinator is elected
        this.onElectedCoordinator(msg.getMsg().coordinatorElected.newCoordinatorId, msg.getMsg().worstClock);
        // Send an acknowledgment to the sender of the election message
        this.sendAckToSender(msg);
        // If an election is in progress, reset the election state and forward the message to the next replica
        if (!this.isElectionInProgress()) {
            return;
        }
        // Reset the election state and forward the message to the next replica
        this.electionInProgress = null;
        // Avoid infinite loops by checking if the message is from this replica
        if (msg.getMsg().coordinatorElected.replicaId == this.id) {
            return;
        }
        this.sendToNextReplica(msg);
        if (this.syncMessage != null) {
            this.onSyncMessage(this.syncMessage);
        }
    }
    /**
     * Send an acknowledgment message to the sender of an election message.
     * @param msg the election message
     */
    private void sendAckToSender(MessageWithACK msg) {
        /** Acknowledgment message */
        Serializable ack = msg.getACK();
        /** Sender ID */
        Integer senderId = msg.senderId;
        if (senderId != null && this.replicas.containsKey(senderId)) {
            ActorRef senderReplica = this.replicas.get(senderId);
            debug("Sending ack to: "+senderReplica.path().name() +" for message:" + msg +"|"+ msg.msg.toString()+"|");
            senderReplica.tell(ack, this.getSelf());
        }
    }
    /**
     * Handle an election message by updating the election state and
     * forwarding the message to the next replica.
     * @param msg the election message
     */
    private void onElectionMessage(Election msg) {
        debug("Recived electionMSG:" + msg.toString()+"|"+msg.msg.toString()+"|"+" from: "+getSender().path().name());
        if (msg.getMsg().isElectionOver(electionInProgress)) {
            /** Best candidate for coordinator */
            Integer bestCandidate = msg.getMsg().getBestCandidate();
            if (bestCandidate != null) {
                this.replicas = msg.getMsg().deleteCrashedNodesFromList(this.replicas);
                /** Coordinator elected message */
                ElectionOver coordinatorElected = new ElectionOver(
                        this.id, 
                        new ElectionChooseCoordinator(
                                new CoordinatorElected(bestCandidate, this.id), 
                                msg.getMsg().getWorstClock()
                            )
                    );
                this.sendToNextReplica(coordinatorElected);
                this.newCoordinator(bestCandidate);
            }
        } else if (!this.isElectionInProgress() || msg.getMsg().electionStarter < this.electionInProgress) {
            log("I'm not ignoring this message");
            if (!this.isElectionInProgress()) {
                this.callbackOnElectionStarted(this.coordinatorID);
            }
            this.electionInProgress = msg.getMsg().electionStarter;
            this.coordinatorCrashed();
            msg.updateMsg(this);
            this.sendToNextReplica(msg);
        }
        this.sendAckToSender(msg);
    }
    /**
     * Handle a node crash by removing it from the list of replicas.
     * @param id the id of the crashed node
     */
    private void nodeCrashed(int id){
        try {
            this.replicas.remove((Integer) id);
            if (this.coordinatorID == id) {
                this.onCoordinatorCrash();
            }
        } catch (Exception e) {
            debug("Error while removing crashed node: " + e.getMessage());
        }
    }
    /**
     * Get the next replica ID in a ring topology.
     * @param id the current replica ID
     * @return the next replica ID
     */
    private int correctNextReplicaID(int id) {
        return (id + 1) % this.numberOfReplicas;
    }
    /**
     * Get the next alive replica ID in a ring topology, skipping crashed nodes.
     * @return the next alive replica ID
     */
    private synchronized int getNextAliveReplicaID() {
        if (this.replicas == null || this.replicas.isEmpty()) {
            debug("No replicas available. Returning -1 as next alive replica ID.");
            return this.nextReplicaID;
        }
        if (this.nextReplicaID == null) {
            this.nextReplicaID = this.correctNextReplicaID(this.id);
        }
        while (!this.replicas.containsKey(this.nextReplicaID)) {
            this.nextReplicaID = this.correctNextReplicaID(this.nextReplicaID);
        }
        debug("My next replica is: "+ this.nextReplicaID);
        return this.nextReplicaID;
    }
    /**
     * Handle an election timeout.
     */
    private void onElectionTimeout(TimeOut msg) {
        debug("Election timeout. Removing the next replica from the list of replicas.");
        // get and remove the first inserted ElectionMessage in the electionTimeouts map
        if (this.nextReplicaID == null) {
            this.nextReplicaID = this.getNextAliveReplicaID();
        } else {
            this.nodeCrashed(this.nextReplicaID);
            this.nextReplicaID = this.getNextAliveReplicaID();
        }
        if (this.electionTimeouts != null && !this.electionTimeouts.isEmpty()) {
            Serializable first = this.electionTimeouts.keySet().iterator().next();
            this.electionTimeouts.remove(first);
            MessageWithACK electionMessage = new Election(this.id, (ElectionMessage) first);
            this.sendToNextReplica(electionMessage);
        }
    }
    /**
     * Handle an election acknowledgment message by canceling the corresponding timeout.
     * @param msg the election acknowledgment message
     */
    private void onElectionAck(ElectionAck msg) {
        debug("Received an ElectionAck for message "+ msg.electionMessage.toString());
        this.electionTimeouts.get(msg.electionMessage).remove().cancel();
    }
    /**
     * Send a message to the next alive replica in a ring topology.
     * @param msg the message to send
     */
    private void sendToNextReplica(MessageWithACK msg) {
        ActorRef nextReplica = this.replicas.get(this.getNextAliveReplicaID());
        MessageWithACK updatedMsg = msg.updateSender(this.id);
        debug("The message is an instance of class: " + msg.getClass() + " and is a " +msg.toString());
        nextReplica.tell(updatedMsg, this.getSelf());
        this.electionTimeouts.computeIfAbsent(updatedMsg.msg, k -> new ArrayDeque<>())
                .add(setTimeout(this.getMaxLatencyPlusTolerance(),new TimeOut(TimeoutType.Election)));
        // CRASH ELECTION
        this.checkIfTimeToCrash(Crash.Type.Election);
    }
    /**
     * Start an election.
     */
    private void startElection() {
        debug("ELECTION STARTED!!!!");
    // ElectionMessage electionMessage = new ElectionMessage(this.id, this.updateClock);
        Election election = new Election(this.id, this.waitingForWriteOkUpdateClock.clone());
        if (this.isElectionInProgress() && this.electionInProgress <= this.id) {
            // An election is already in progress
            return;
        }
        this.electionInProgress = this.id;
        this.callbackOnElectionStarted(this.coordinatorID);
        this.sendToNextReplica(election);
        debug("Starting an election with message: " + election.toString());
    }
    /**
     * Handle a heartbeat message from the coordinator.
     * @param msg the heartbeat message
     */
    private void onHeartbeat(CoordinatorHeartbeat msg) {
        /** Handle a heartbeat message from the coordinator */
        long timeout = (long)COORDINATOR_BEAT_INTERVAL;
        // If this replica is the coordinator
        // set half the timeout to let it expire faster and trigger its own heartbeat
        if (this.isCoordinator()) {
            timeout /= 2;
        }
        // Cancel the previous heartbeat timeout
        if (this.coordinatorHeartbeatTimeout != null) {
            this.coordinatorHeartbeatTimeout.cancel();
        }
        // Set a new heartbeat timeout
        this.coordinatorHeartbeatTimeout = this.setTimeout(timeout, new TimeOut(TimeoutType.Heartbeat));
    }
    /**
     * Handle a heartbeat timeout.
     * If the coordinator is this replica, send a heartbeat message.
     * Otherwise, handle the coordinator crash.
     */
    private void onHeartbeatTimeout() {
        // If the coordinator is this replica, send a heartbeat message
        if (this.isCoordinator()) {
            this.sendHeartbeat();
        }
        // Otherwise, handle the coordinator crash 
        else {
            this.coordinatorCrashed();
        }
    }
    /**
     * Send a heartbeat message to all replicas.
     */
    private void sendHeartbeat() {
        this.multicast(new CoordinatorHeartbeat(this.id), Crash.Type.Heartbeat);
    }


    private void onSyncMessage(Synchronization msg) {
        if (this.isElectionInProgress()) {
            this.syncMessage = msg;
            return;
        }
        for (UpdateClock updateClock : msg.missingHistory.keySet()) {
            if (this.updateClock.compareTo(updateClock) > 0 ) {
                continue;
            }
            AbstractClient.WriteRequest writeRequest = retrieveWriteRequest(msg, updateClock);
            this.updateClock.syncClock(updateClock);
            // ACK the client
            getPendingWrite(writeRequest);
        }
        this.waitingForWriteOkUpdateClock.syncClock(this.updateClock);
        this.cancelAllWriteRequestTimeOut();
        getContext().become(createReceive());
        while (!this.updateRequestQueue.isEmpty()) {
            UpdateRequest m = this.updateRequestQueue.remove();
            this.onUpdateRequest(m);
        }
        while (!this.writeOKQueue.isEmpty()) {
            WriteOK m = this.writeOKQueue.remove();
            this.onWriteOK(m);
        }
        debug("HEY! "+this.coordinatorID);
        for (Replica.ClientWrite w : this.pendingWrites) {
            replicas.get(this.coordinatorID).tell(w.writeRequest, this.getSelf());
            this.writeRequestTimeouts.computeIfAbsent(w.writeRequest, k -> new ArrayDeque<>())
                    .add(setTimeout(this.TIMEOUT_DELAY, new TimeOut(TimeOut.TimeoutType.WriteRequest)));
        }
        this.syncMessage = null;
    }
    /**
     * Retrieve a write request from the synchronization message and update the replica's state accordingly.
     * @param msg the synchronization message containing the missing history
     * @param updateClock the update clock associated with the write request to retrieve
     * @return the retrieved write request
     */
    private AbstractClient.WriteRequest retrieveWriteRequest(Synchronization msg, UpdateClock updateClock) {
        AbstractClient.WriteRequest writeRequest = msg.missingHistory.get(updateClock);
        this.positions[writeRequest.index] = writeRequest.value;
        this.history.put(updateClock.clone(),new AbstractClient.WriteRequest(writeRequest.index,writeRequest.value,writeRequest.replica));
        return writeRequest;
    }
    /**
     * Check if there is a pending write request that matches the given write request and send an acknowledgment to the client if found.
     * @param writeRequest the write request to check for pending writes
     */
    private void getPendingWrite(AbstractClient.WriteRequest writeRequest) {
        Optional<ClientWrite> pendingWrite = this.pendingWrites.stream()
                .filter(p -> (p.writeRequest.index == writeRequest.index && p.writeRequest.value == writeRequest.value))
                .findFirst();
        if (pendingWrite.isPresent()) {
            pendingWrite.get().clientRef.tell(
                    new Replica.ClientACK(this.getSelf(), new AbstractClient.WriteResult(true, writeRequest.index, writeRequest.value, this.id)),
                    this.getSelf());
            this.pendingWrites.remove(pendingWrite.get());
        }
    }

 private void updateHistory() {
        List<UpdateClock> ordered_clock = this.waitingForWriteOK.keySet().stream().sorted().toList();
        for (UpdateClock clk : ordered_clock) {
            AbstractClient.WriteRequest writeRequest = this.waitingForWriteOK.get(clk);
            this.positions[writeRequest.index] = writeRequest.value;
            this.history.put(clk.clone(), new AbstractClient.WriteRequest(writeRequest.index, writeRequest.value, writeRequest.replica));
            // ACK the client
            this.getPendingWrite(writeRequest);
        }
        cancelAllUpdateRequestTimeOut();
        this.waitingForWriteOK.clear();
    }

    private void cancelAllUpdateRequestTimeOut() {
        for (Cancellable timeout : this.updateRequestTimeouts.values()) {
            timeout.cancel();
        }
        this.updateRequestTimeouts.clear();

    }
    private void cancelAllWriteRequestTimeOut() {
        for (Queue<Cancellable> timeout : this.writeRequestTimeouts.values()) {
            for (Cancellable t : timeout) {
                t.cancel();
            }
        }
        this.writeRequestTimeouts.clear();

    }
}
