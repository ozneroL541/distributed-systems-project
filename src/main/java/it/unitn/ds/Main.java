package it.unitn.ds;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

import akka.actor.ActorRef;
import akka.actor.ActorSystem;
import it.unitn.ds.AbstractReplica.InitSystem;

public class Main {

    public static void main(String[] args) throws InterruptedException {
        System.out.println("========================================");
        System.out.println("START");
        System.out.println("========================================\n");

        final int N_REPLICAS = 6;
        final int COORDINATOR_ID = 0;
        final ActorSystem system = ActorSystem.create("TestMain");

        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);

        verifyTotalOrderOnCoordinatorCrash();
        crashOnWriteOK();

        System.out.println("\n========================================");
        System.out.println("END");
        System.out.println("========================================\n");
        system.terminate();
    }

    private static void verifyTotalOrderOnCoordinatorCrash() throws InterruptedException {
        System.out.println("================================================");
        System.out.println("Verify total order and sync on coordinator crash");
        System.out.println("================================================\n");
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        final int N_REPLICAS = 21;
        final int COORDINATOR_ID = 8;
        final int N_CLIENT = 10;
        final ActorSystem system = ActorSystem.create("verifyTotalOrderOnCoordinatorCrash");

        Map<Integer, ActorRef> replica = createReplica(system, N_REPLICAS, COORDINATOR_ID);
        Map<Integer, ActorRef> clients = createClients(system, N_CLIENT);

        replica.get(8).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.Update, 5), ActorRef.noSender());
        Thread.sleep(100);
        clients.get(0).tell(new AbstractClient.WriteRequest(8, 88, replica.get(7)), ActorRef.noSender());
        clients.get(1).tell(new AbstractClient.WriteRequest(7, 77, replica.get(4)), ActorRef.noSender());
        clients.get(2).tell(new AbstractClient.WriteRequest(2, 22, replica.get(1)), ActorRef.noSender());
        clients.get(3).tell(new AbstractClient.WriteRequest(12, 12, replica.get(11)), ActorRef.noSender());
        clients.get(4).tell(new AbstractClient.WriteRequest(13, 12, replica.get(11)), ActorRef.noSender());
        Thread.sleep(1000);
        System.out.println("## READ RESULT ##");
        clients.get(0).tell(new AbstractClient.ReadRequest(8, replica.get(18)), ActorRef.noSender());
        clients.get(1).tell(new AbstractClient.ReadRequest(7, replica.get(18)), ActorRef.noSender());
        clients.get(2).tell(new AbstractClient.ReadRequest(2, replica.get(18)), ActorRef.noSender());
        clients.get(3).tell(new AbstractClient.ReadRequest(12, replica.get(18)), ActorRef.noSender());
        clients.get(4).tell(new AbstractClient.ReadRequest(13, replica.get(18)), ActorRef.noSender());
        system.terminate();
    }


    private static void crashOnWriteOK() throws InterruptedException {
        System.out.println("=================================================================");
        System.out.println("Crash coordinator on writeOK, verify that the update is preserved");
        System.out.println("=================================================================\n");
        Logger.setDestinationStdout();
        Logger.setDebugEnabled(true);
        final int N_REPLICAS = 5;
        final int COORDINATOR_ID = 0;
        final int N_CLIENT = 3;
        final ActorSystem system = ActorSystem.create("verifyWriteOK");

        Map<Integer, ActorRef> replica = createReplica(system, N_REPLICAS, COORDINATOR_ID);
        Map<Integer, ActorRef> clients = createClients(system, N_CLIENT);

        replica.get(0).tell(new AbstractReplica.Crash(AbstractReplica.Crash.Type.WriteOK, 2), ActorRef.noSender());
        Thread.sleep(100);
        clients.get(0).tell(new AbstractClient.WriteRequest(2, 2, replica.get(1)), ActorRef.noSender());
        clients.get(1).tell(new AbstractClient.WriteRequest(3, 33, replica.get(2)), ActorRef.noSender());
        Thread.sleep(1000);
        System.out.println("## READ RESULT ##");
        clients.get(0).tell(new AbstractClient.ReadRequest(2, replica.get(2)), ActorRef.noSender());
        clients.get(0).tell(new AbstractClient.ReadRequest(3, replica.get(3)), ActorRef.noSender());
        system.terminate();
    }


    private static Map<Integer, ActorRef> createReplica(ActorSystem system, int N_REPLICAS, int COORDINATOR_ID) {
        Map<Integer, ActorRef> replicas = new HashMap<>(N_REPLICAS);
        for (int i = 0; i < N_REPLICAS; i++) {
            replicas.put(i,
                    system.actorOf(
                            Replica.props(i, AbstractReplica.MIN_LATENCY, AbstractReplica.MAX_LATENCY, AbstractReplica.COORDINATOR_BEAT_INTERVAL),
                            "Replica_" + i
                    )
            );
        }
        InitSystem initMsg = new InitSystem(replicas, COORDINATOR_ID);
        for (Map.Entry<Integer, ActorRef> entry : replicas.entrySet()) {
            entry.getValue().tell(initMsg, ActorRef.noSender());
        }
        return replicas;
    }

    private static Map<Integer, ActorRef> createClients(ActorSystem system, int N_CLIENT) {
        Map<Integer, ActorRef> clients = new HashMap<>(N_CLIENT);
        for (int i = 0; i < N_CLIENT; i++) {
            Optional<ActorRef> defaultTargetReplica = Optional.empty();
            clients.put(i,
                    system.actorOf(
                            Client.props(500, 1000, defaultTargetReplica),
                            "Client_" + i));
        }
        return clients;
    }


}
