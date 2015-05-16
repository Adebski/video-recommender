package ztis.user_video_service;

import org.neo4j.graphdb.GraphDatabaseService;
import org.neo4j.graphdb.Transaction;
import scala.Function0;

public class UnitOfWorkHelper {

    public static <T> T performTransactionally(Function0<T> action, GraphDatabaseService graphDatabaseService) {
        T result;

        try (Transaction tx = graphDatabaseService.beginTx()) {
            result = action.apply();
            tx.success();
        }

        return result;
    }
}
