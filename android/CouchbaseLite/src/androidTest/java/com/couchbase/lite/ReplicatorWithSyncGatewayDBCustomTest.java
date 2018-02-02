package com.couchbase.lite;


import com.couchbase.lite.internal.support.Log;

import junit.framework.Assert;

import org.junit.Test;

import java.io.IOException;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.ArrayList;
import java.util.List;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.TimeUnit;

import okhttp3.MediaType;
import okhttp3.OkHttpClient;
import okhttp3.RequestBody;
import okhttp3.Response;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

public class ReplicatorWithSyncGatewayDBCustomTest extends BaseReplicatorTest {
    private static final MediaType JSON = MediaType.parse("application/json; charset=utf-8");
    private static final String REMOTE_DB_NAME = "db";
    private static final int REPEAT = 10;


    @Test
    public void testDiskIOError1Docs() throws CouchbaseLiteException, InterruptedException, URISyntaxException, IOException {
        for (int i = 0; i < REPEAT; i++)
            testDiskIOError(1, 10);
    }

    @Test
    public void testDiskIOError5Docs() throws CouchbaseLiteException, InterruptedException, URISyntaxException, IOException {
        for (int i = 0; i < REPEAT; i++)
            testDiskIOError(5, 10);
    }

    @Test
    public void testDiskIOError10Docs() throws CouchbaseLiteException, InterruptedException, URISyntaxException, IOException {
        for (int i = 0; i < REPEAT; i++)
            testDiskIOError(10, 10);
    }

    @Test
    public void testDiskIOError50Docs() throws CouchbaseLiteException, InterruptedException, URISyntaxException, IOException {
        for (int i = 0; i < REPEAT; i++)
            testDiskIOError(50, 10);
    }

    @Test
    public void testDiskIOError100Docs() throws CouchbaseLiteException, InterruptedException, URISyntaxException, IOException {
        for (int i = 0; i < REPEAT; i++)
            testDiskIOError(100, 10);
    }

    @Test
    public void testDiskIOError200Docs() throws CouchbaseLiteException, InterruptedException, URISyntaxException, IOException {
        for (int i = 0; i < REPEAT; i++)
            testDiskIOError(200, 10);
    }

    void testDiskIOError(final int numDocs, final int numUpdate) throws CouchbaseLiteException, InterruptedException, URISyntaxException, IOException {

        if (!config.replicatorTestsEnabled())
            return;

        remote_PUT_db(REMOTE_DB_NAME);

        Database db1 = null;
        Database db2 = null;
        Database db3 = null;
        try {
            // Create CBL databases
            DatabaseConfiguration conf = new DatabaseConfiguration(context);
            db1 = new Database("cbl_db1", conf);
            db2 = new Database("cbl_db2", conf);
            db3 = new Database("cbl_db3", conf);

            // Create docs
            final List<String> docIDs1 = createDocs(db1, "cbl_db1", numDocs);
            final List<String> docIDs2 = createDocs(db2, "cbl_db2", numDocs);
            final List<String> docIDs3 = createDocs(db3, "cbl_db3", numDocs);

            // Replicate from DB1 to DB2 and DB3
            Replicator r1to2 = new Replicator(makeConfig(db1, db2, true));
            Replicator r1to3 = new Replicator(makeConfig(db1, db3, true));
            CountDownLatch latch1 = new CountDownLatch(1);
            CountDownLatch latch2 = new CountDownLatch(1);
            ListenerToken token1 = r1to2.addChangeListener(executor, new IdleReplicatorChangeListener(latch1));
            ListenerToken token2 = r1to3.addChangeListener(executor, new IdleReplicatorChangeListener(latch2));
            r1to2.start();
            r1to3.start();

            // update thread from differnt thread
            final CountDownLatch updateLatch1 = new CountDownLatch(1);
            final CountDownLatch updateLatch2 = new CountDownLatch(1);
            final CountDownLatch updateLatch3 = new CountDownLatch(1);
            createUpdateThread(db1, numUpdate, docIDs1, updateLatch1).start();
            //createUpdateThread(db2, numUpdate, docIDs2, updateLatch2).start();
            //createUpdateThread(db3, numUpdate, docIDs3, updateLatch3).start();
            assertTrue(updateLatch1.await(300, TimeUnit.SECONDS));
            //assertTrue(updateLatch2.await(300, TimeUnit.SECONDS));
            //assertTrue(updateLatch3.await(300, TimeUnit.SECONDS));

            // wait till repl becomes idle
            assertTrue(latch1.await(300, TimeUnit.SECONDS));
            assertTrue(latch2.await(300, TimeUnit.SECONDS));
            r1to2.removeChangeListener(token1);
            r1to3.removeChangeListener(token2);

            // Stop replicators
            CountDownLatch latch3 = new CountDownLatch(1);
            CountDownLatch latch4 = new CountDownLatch(1);
            ListenerToken token3 = r1to2.addChangeListener(executor, new StopReplicatorChangeListener(latch3));
            ListenerToken token4 = r1to3.addChangeListener(executor, new StopReplicatorChangeListener(latch4));
            r1to2.stop();
            r1to3.stop();
            assertTrue(latch3.await(120, TimeUnit.SECONDS));
            assertTrue(latch4.await(120, TimeUnit.SECONDS));
            r1to2.removeChangeListener(token3);
            r1to3.removeChangeListener(token4);

            // check all 3 databases have same number of documents
            assertEquals(numDocs * 3, db1.getCount());
            assertEquals(numDocs * 3, db2.getCount());
            assertEquals(numDocs * 3, db3.getCount());

            // Replicate from DB3 to SG
            Replicator r3toSG = new Replicator(makeConfig(db3, getRemoteURI(REMOTE_DB_NAME, false), true));
            CountDownLatch latch5 = new CountDownLatch(1);
            ListenerToken token5 = r3toSG.addChangeListener(executor, new IdleReplicatorChangeListener(latch5));
            r3toSG.start();
            assertTrue(latch5.await(120, TimeUnit.SECONDS));
            r3toSG.removeChangeListener(token5);

            // Stop replicators
            CountDownLatch latch6 = new CountDownLatch(1);
            ListenerToken token6 = r3toSG.addChangeListener(executor, new StopReplicatorChangeListener(latch6));
            r3toSG.stop();
            assertTrue(latch6.await(120, TimeUnit.SECONDS));
            r3toSG.removeChangeListener(token6);
        } finally {
            // tear down

            remote_DELETE_db(REMOTE_DB_NAME);

            int i;
            for (i = 0; i < 6; i++) { // 1min
                try {
                    db1.delete();
                    break;
                } catch (CouchbaseLiteException e) {
                    Log.e(TAG, "Failed in Databae.delete() - " + db1, e);
                    Thread.sleep(1000 * 10); // 10 sec
                }
            }
            if (i == 6)
                Assert.fail("Unable to delete database: " + db1);

            for (i = 0; i < 6; i++) { // 1min
                try {
                    db2.delete();
                    break;
                } catch (CouchbaseLiteException e) {
                    Log.e(TAG, "Failed in Databae.delete() - " + db2, e);
                    Thread.sleep(1000 * 10); // 10 sec
                }
            }
            if (i == 6)
                Assert.fail("Unable to delete database: " + db2);
            for (i = 0; i < 6; i++) {
                try {
                    db3.delete();
                    break;
                } catch (CouchbaseLiteException e) {
                    Log.e(TAG, "Failed in Databae.delete() - " + db3, e);
                    Thread.sleep(1000 * 10); // 10 sec
                }
            }
            if (i == 6)
                Assert.fail("Unable to delete database: " + db3);
        }
    }

    static class IdleReplicatorChangeListener implements ReplicatorChangeListener {
        CountDownLatch latch;

        IdleReplicatorChangeListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void changed(ReplicatorChange change) {
            Replicator.Status status = change.getStatus();
            Replicator repl = change.getReplicator();
            CouchbaseLiteException error = status.getError();
            final String kActivityNames[] = {"stopped", "offline", "connecting", "idle", "busy"};
            Log.i(TAG, "--- Status: %s %s (%d / %d), lastError = %s",
                    repl,
                    kActivityNames[status.getActivityLevel().getValue()],
                    status.getProgress().getCompleted(), status.getProgress().getTotal(),
                    error);

            if (status.getActivityLevel() == Replicator.ActivityLevel.IDLE
                    && status.getProgress().getCompleted() == status.getProgress().getTotal())
                latch.countDown();
        }
    }

    static class StopReplicatorChangeListener implements ReplicatorChangeListener {
        CountDownLatch latch;

        StopReplicatorChangeListener(CountDownLatch latch) {
            this.latch = latch;
        }

        @Override
        public void changed(ReplicatorChange change) {
            Replicator.Status status = change.getStatus();
            CouchbaseLiteException error = status.getError();
            final String kActivityNames[] = {"stopped", "offline", "connecting", "idle", "busy"};
            Log.i(TAG, "--- Status: %s (%d / %d), lastError = %s",
                    kActivityNames[status.getActivityLevel().getValue()],
                    status.getProgress().getCompleted(), status.getProgress().getTotal(),
                    error);

            if (status.getActivityLevel() == Replicator.ActivityLevel.STOPPED)
                latch.countDown();
        }
    }

    ReplicatorConfiguration makeConfig(Database source, Database target, boolean continuous) {
        return new ReplicatorConfiguration(source, new DatabaseEndpoint(target))
                .setContinuous(continuous)
                .setReplicatorType(ReplicatorConfiguration.ReplicatorType.PUSH_AND_PULL);
    }

    ReplicatorConfiguration makeConfig(Database source, URI target, boolean continuous) {
        return new ReplicatorConfiguration(source, new URLEndpoint(target))
                .setContinuous(continuous)
                .setReplicatorType(ReplicatorConfiguration.ReplicatorType.PUSH_AND_PULL);
    }

    List<String> createDocs(Database database, String prefix, int n) throws CouchbaseLiteException {
        List<String> docs = new ArrayList<>();
        for (int i = 0; i < n; i++) {
            MutableDocument mDoc = createMutableDocument(String.format(Locale.US, "%s_doc_%03d", prefix, i));
            mDoc.setValue("key", i);
            Document doc = database.save(mDoc);
            docs.add(doc.getId());
        }
        assertEquals(n, database.getCount());
        return docs;
    }

    Thread createUpdateThread(final Database db, final int numUpdate, final List<String> docIDs, final CountDownLatch latch){
        return new Thread(new Runnable() {
            @Override
            public void run() {
                for(int i = 1; i <= numUpdate; i++){
                    for(String docID : docIDs){
                        MutableDocument mDoc = db.getDocument(docID).toMutable();
                        mDoc.setInt("updated", i);
                        try {
                            db.save(mDoc);
                        } catch (CouchbaseLiteException e) {
                            Log.e(TAG, "Failed to execute Database.save() with " + mDoc, e);
                            org.junit.Assert.fail();
                        }
                    }
                }
                latch.countDown();
            }
        });
    }

    void shutdownAndAwaitTermination(ExecutorService pool) {
        pool.shutdown(); // Disable new tasks from being submitted
        try {
            // Wait a while for existing tasks to terminate
            if (!pool.awaitTermination(60, TimeUnit.SECONDS)) {
                pool.shutdownNow(); // Cancel currently executing tasks
                // Wait a while for tasks to respond to being cancelled
                if (!pool.awaitTermination(60, TimeUnit.SECONDS))
                    System.err.println("Pool did not terminate");
            }
        } catch (InterruptedException ie) {
            // (Re-)Cancel if current thread also interrupted
            pool.shutdownNow();
            // Preserve interrupt status
            Thread.currentThread().interrupt();
        }
    }

    private boolean remote_PUT_db(String db) throws IOException {
        OkHttpClient client = new OkHttpClient();
        String url = String.format(Locale.ENGLISH, "http://%s:4985/%s/", this.config.remoteHost(), db);
        RequestBody body = RequestBody.create(JSON, "{\"server\": \"walrus:\", \"users\": { \"GUEST\": { \"disabled\": false, \"admin_channels\": [\"*\"] } }, \"unsupported\": {\"replicator_2\":true}}");
        okhttp3.Request request = new okhttp3.Request.Builder()
                .url(url)
                .put(body)
                .build();
        Response response = client.newCall(request).execute();
        return response.code() >= 200 && response.code() < 300;
    }

    private boolean remote_DELETE_db(String db) throws IOException {
        OkHttpClient client = new OkHttpClient();
        String url = String.format(Locale.ENGLISH, "http://%s:4985/%s/", this.config.remoteHost(), db);
        okhttp3.Request request = new okhttp3.Request.Builder()
                .url(url)
                .delete()
                .build();
        Response response = client.newCall(request).execute();
        return response.code() >= 200 && response.code() < 300;
    }
}
