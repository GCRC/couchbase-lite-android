package com.couchbase.lite;

import org.junit.Test;

import java.util.Date;
import java.util.Locale;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicInteger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class ConcurrentDocumentTest extends BaseTest {
    private static final String TAG = ConcurrentDocumentTest.class.getSimpleName();

    Document createDocument(int i) {
        // docID
        String docID = String.format(Locale.ENGLISH, "doc%d", i);
        Document doc = new Document(docID);

        // index
        doc.setInt("index", i);
        doc.setBoolean("even", i % 2 == 0 ? true : false);

        // String
        doc.setObject("firstName", "Daniel");
        doc.setObject("lastName", "Tiger");

        // Dictionary:
        Dictionary address = new Dictionary();
        address.setObject("street", String.format(Locale.ENGLISH, "%d Main street", i));
        address.setObject("city", "Mountain View");
        address.setObject("state", "CA");
        doc.setObject("address", address);

        // Array:
        Array phones = new Array();
        phones.addObject(String.format(Locale.ENGLISH, "650-123-%04d", i));
        phones.addObject(String.format(Locale.ENGLISH, "650-123-%04d", i + 1));
        doc.setObject("phones", phones);

        // Date:
        doc.setObject("updated", new Date());

        return doc;
    }

    Document updateDocument(final Document doc, final int i) {
        doc.setInt("update", i);

        doc.setBoolean("even", i % 2 == 0 ? true : false);

        // String
        doc.setObject("firstName", String.format(Locale.ENGLISH, "Daniel-%d", i));
        doc.setObject("lastName", String.format(Locale.ENGLISH, "Tiger-%d", i));

        // Dictionary:
        Dictionary address = new Dictionary();
        address.setObject("street", String.format(Locale.ENGLISH, "%d first street", i));
        address.setObject("city", "Mountain View");
        address.setObject("state", "CA");
        doc.setObject("address", address);

        // Array:
        Array phones = new Array();
        phones.addObject(String.format(Locale.ENGLISH, "1-650-123-%04d", i));
        phones.addObject(String.format(Locale.ENGLISH, "1-650-123-%04d", i + 1));
        doc.setObject("phones", phones);

        // Date:
        doc.setObject("updated", new Date());

        return doc;
    }

    void saveDocument(Document doc) {
        try {
            db.save(doc);
        } catch (CouchbaseLiteException e) {
            Log.e(TAG, "Error in Database.save() docID -> %s", e, doc.getId());
            fail();
        }
    }

    static class ConcurrentValidatorRunnable implements Runnable {
        int iteration;
        Validator<Document> dataValidator;
        CountDownLatch latch;
        Document doc;

        ConcurrentValidatorRunnable(Document doc, Validator<Document> dataValidator,
                                    CountDownLatch latch, final int iteration) {
            this.doc = doc;
            this.dataValidator = dataValidator;
            this.latch = latch;
            this.iteration = iteration;
        }

        @Override
        public void run() {
            for (int i = 0; i < iteration; i++) {
                dataValidator.validate(doc);
            }
            latch.countDown();
        }
    }

    private void concurrentValidator(final Document doc, final Validator<Document> dataValidator, int nThreads, final int nIteration) {
        // setup
        final Thread[] threads = new Thread[nThreads];
        final CountDownLatch[] latchs = new CountDownLatch[nThreads];
        for (int i = 0; i < nThreads; i++) {
            latchs[i] = new CountDownLatch(1);
            threads[i] = new Thread(new ConcurrentValidatorRunnable(doc, dataValidator, latchs[i], nIteration));
        }

        // start
        for (int i = 0; i < nThreads; i++)
            threads[i].start();

        // wait
        for (int i = 0; i < nThreads; i++) {
            try {
                assertTrue(latchs[i].await(60, TimeUnit.SECONDS));
            } catch (InterruptedException e) {
                Log.e(TAG, "InterruptedException", e);
                fail();
            }
        }
    }

    @Test
    public void testConcurrentGetFromDocument() throws CouchbaseLiteException, InterruptedException {
        if (!config.concurrentTestsEnabled())
            return;

        // TODO: By increasing following values, this test causes the crash. I guess there is memory leak.

        final int kNThreads = 10;
        final int kNIteration = 20;

        final Validator<Document> dataValidator = new Validator<Document>() {
            @Override
            public void validate(Document doc) {
                assertEquals("doc1", doc.getId());

                assertEquals("Daniel", doc.getString("firstName"));
                assertEquals("Tiger", doc.getString("lastName"));

                assertEquals(1, doc.getInt("index"));
                assertEquals(1, doc.getLong("index"));
                assertEquals(1.0F, doc.getFloat("index"), 0.0F);
                assertEquals(1.0, doc.getDouble("index"), 0.0);
                assertEquals(false, doc.getBoolean("even"));
                assertNotNull(doc.getDate("updated"));

                Dictionary address = doc.getDictionary("address");
                assertNotNull(address);
                assertEquals("1 Main street", address.getString("street"));
                assertEquals("Mountain View", address.getString("city"));
                assertEquals("CA", address.getString("state"));

                Array phones = doc.getArray("phones");
                assertNotNull(phones);
                assertEquals("650-123-0001", phones.getString(0));
                assertEquals("650-123-0002", phones.getString(1));
            }
        };

        // create Document instance
        Document doc = createDocument(1);

        // before save validation, concurrently access data (Java objects) in Document
        concurrentValidator(doc, dataValidator, kNThreads, kNIteration);

        // save
        db.save(doc);

        // after save validation, concurrently access data (fleece data) in Document
        concurrentValidator(doc, dataValidator, kNThreads, kNIteration);
    }

    @Test
    public void testConcurrentSetToDocument() throws CouchbaseLiteException, InterruptedException {
        if (!config.concurrentTestsEnabled())
            return;

        final int kNThreads = 10;
        final int kNIteration = 20;
        final AtomicInteger index = new AtomicInteger(0);

        final Validator<Document> updateValidator = new Validator<Document>() {
            @Override
            public void validate(Document doc) {
                updateDocument(doc, index.incrementAndGet());
            }
        };

        final Validator<Document> dataValidator = new Validator<Document>() {
            @Override
            public void validate(Document doc) {
                // document ID
                assertEquals("doc1", doc.getId());

                // string
                assertEquals("Daniel-" + index.intValue(), doc.getString("firstName"));
                assertEquals("Tiger-" + index.intValue(), doc.getString("lastName"));

                // not updated
                assertEquals(1, doc.getInt("index"));
                assertEquals(1, doc.getLong("index"));
                assertEquals(1.0F, doc.getFloat("index"), 0.0F);
                assertEquals(1.0, doc.getDouble("index"), 0.0);

                // updated int value
                assertEquals(index.intValue(), doc.getInt("update"));
                assertEquals(index.longValue(), doc.getLong("update"));
                assertEquals(index.floatValue(), doc.getFloat("update"), 0.0F);
                assertEquals(index.doubleValue(), doc.getDouble("update"), 0.0);

                // boolean
                assertEquals(index.intValue() % 2 == 0 ? true : false, doc.getBoolean("even"));

                // date
                assertNotNull(doc.getDate("updated"));

                // dictionary
                Dictionary address = doc.getDictionary("address");
                assertNotNull(address);
                assertEquals(String.valueOf(index.intValue()) + " first street", address.getString("street"));
                assertEquals("Mountain View", address.getString("city"));
                assertEquals("CA", address.getString("state"));

                // array
                Array phones = doc.getArray("phones");
                assertNotNull(phones);
                assertEquals(String.format(Locale.ENGLISH, "1-650-123-%04d", index.intValue()), phones.getString(0));
                assertEquals(String.format(Locale.ENGLISH, "1-650-123-%04d", index.intValue() + 1), phones.getString(1));
            }
        };

        // create Document instance
        Document doc = createDocument(1);

        // concurrent Document update before save
        concurrentValidator(doc, updateValidator, kNThreads, kNIteration);

        // NOTE: With multi-threads update, the last update does not gurantee index value is last.
        //       Following validation might be meaning less

        // validate data in the document
        updateValidator.validate(doc);
        dataValidator.validate(doc);

        // save
        db.save(doc);

        doc = db.getDocument("doc1");
        dataValidator.validate(doc);

        // concurrent Document update after save
        doc = db.getDocument("doc1");
        concurrentValidator(doc, updateValidator, kNThreads, kNIteration);

        // NOTE: With multi-threads update, the last update does not gurantee index value is last.
        //       Following validation might be meaning less

        // validate data in the document
        updateValidator.validate(doc);
        dataValidator.validate(doc);
    }

    @Test
    public void testConcurrentSetNSaveDocument() throws CouchbaseLiteException, InterruptedException {
        if (!config.concurrentTestsEnabled())
            return;

        final int kNThreads = 10;
        final int kNIteration = 20;
        final AtomicInteger index = new AtomicInteger(0);

        final Validator<Document> updateNSavevalidator = new Validator<Document>() {
            @Override
            public void validate(Document doc) {
                Document updatedDoc = updateDocument(doc, index.incrementAndGet());
                saveDocument(updatedDoc);
            }
        };

        final Validator<Document> dataValidator = new Validator<Document>() {
            @Override
            public void validate(Document doc) {
                // document ID
                assertEquals("doc1", doc.getId());

                // not updated
                assertEquals(1, doc.getInt("index"));
                assertEquals(1, doc.getLong("index"));
                assertEquals(1.0F, doc.getFloat("index"), 0.0F);
                assertEquals(1.0, doc.getDouble("index"), 0.0);

                // string
                assertEquals("Daniel-" + index.intValue(), doc.getString("firstName"));
                assertEquals("Tiger-" + index.intValue(), doc.getString("lastName"));

                // updated int value
                assertEquals(index.intValue(), doc.getInt("update"));
                assertEquals(index.longValue(), doc.getLong("update"));
                assertEquals(index.floatValue(), doc.getFloat("update"), 0.0F);
                assertEquals(index.doubleValue(), doc.getDouble("update"), 0.0);

                // boolean
                assertEquals(index.intValue() % 2 == 0 ? true : false, doc.getBoolean("even"));

                // date
                assertNotNull(doc.getDate("updated"));

                // dictionary
                Dictionary address = doc.getDictionary("address");
                assertNotNull(address);
                assertEquals(String.valueOf(index.intValue()) + " first street", address.getString("street"));
                assertEquals("Mountain View", address.getString("city"));
                assertEquals("CA", address.getString("state"));

                // array
                Array phones = doc.getArray("phones");
                assertNotNull(phones);
                assertEquals(String.format(Locale.ENGLISH, "1-650-123-%04d", index.intValue()), phones.getString(0));
                assertEquals(String.format(Locale.ENGLISH, "1-650-123-%04d", index.intValue() + 1), phones.getString(1));
            }
        };

        // create Document instance
        Document doc = createDocument(1);
        db.save(doc);

        // update & save document
        doc = db.getDocument("doc1");
        concurrentValidator(doc, updateNSavevalidator, kNThreads, kNIteration);

        // NOTE: With multi-threads update, the last update does not gurantee index value is last.
        //       Following validation might be meaning less

        // validate data in the document
        doc = db.getDocument("doc1");
        updateNSavevalidator.validate(doc);
        doc = db.getDocument("doc1");
        dataValidator.validate(doc);

        // update & save document
        doc = db.getDocument("doc1");
        concurrentValidator(doc, updateNSavevalidator, kNThreads, kNIteration);

        // NOTE: With multi-threads update, the last update does not gurantee index value is last.
        //       Following validation might be meaning less

        // validate data in the document
        doc = db.getDocument("doc1");
        updateNSavevalidator.validate(doc);
        doc = db.getDocument("doc1");
        dataValidator.validate(doc);
    }
}
