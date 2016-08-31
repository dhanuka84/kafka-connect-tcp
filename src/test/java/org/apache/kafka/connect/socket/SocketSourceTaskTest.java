package org.apache.kafka.connect.socket;

import org.apache.kafka.connect.data.Struct;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.PrintWriter;
import java.net.Socket;
import java.util.*;

/**
 * Created by Andrea Patelli on 12/02/2016.
 */
public class SocketSourceTaskTest {
    private SocketSourceTask task;

    @Before
    public void setup() {
        task = new SocketSourceTask();
        Map<String, String> configs = new HashMap<>();
        configs.put(SocketSourceConnector.PORT, "12345");
        configs.put(SocketSourceConnector.SCHEMA_NAME, "schematest");
        configs.put(SocketSourceConnector.BATCH_SIZE, "100");
        configs.put(SocketSourceConnector.TOPIC, "topic");
        task.start(configs);
    }

    @Test
    public void testEmpty() throws InterruptedException {
        List<SourceRecord> records = task.poll();
        org.junit.Assert.assertNotNull(records);
        org.junit.Assert.assertEquals(0, records.size());
    }

    @Test
    public void testLessThanBatchSize() throws Exception {
        Socket producer = new Socket("localhost", 12345);
        PrintWriter out = new PrintWriter(producer.getOutputStream(), true);
        List<String> original = new ArrayList<>();
        int items = new Random().nextInt(100);
        for (int i = 0; i < items; i++) {
            String s = UUID.randomUUID().toString();
            out.println(s);
            original.add(s);
            out.flush();
        }
        List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> poll;
        do {
            poll = task.poll();
            org.junit.Assert.assertTrue(poll.size() < 100);
            records.addAll(poll);
        } while (records.size() != items);

        org.junit.Assert.assertEquals(original.size(), records.size());
        for (int i = 0; i < records.size(); i++) {
            String actual = ((Struct) records.get(i).value()).get("message").toString();
            String expected = original.get(i);
            org.junit.Assert.assertEquals(expected, actual);
        }

        out.close();
        producer.close();
    }

    @Test
    public void testMoreThanBatchSize() throws Exception {
        Socket producer = new Socket("localhost", 12345);
        PrintWriter out = new PrintWriter(producer.getOutputStream(), true);
        List<String> original = new ArrayList<>();
        int items = new Random().nextInt(50000) + 100;
        for (int i = 0; i < items; i++) {
            String s = UUID.randomUUID().toString();
            out.println(s);
            original.add(s);
            out.flush();
        }

        List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> poll;
        do {
            poll = task.poll();
            records.addAll(poll);
        } while (poll.size() > 0);

        org.junit.Assert.assertEquals(original.size(), records.size());

        for (int i = 0; i < records.size(); i++) {
            String actual = ((Struct) records.get(i).value()).get("message").toString();
            String expected = original.get(i);
            org.junit.Assert.assertEquals(expected, actual);
        }

        out.close();
        producer.close();
    }

    @Test
    public void testWithDisconnection() throws Exception {
        Socket producer = new Socket("localhost", 12345);
        PrintWriter out = new PrintWriter(producer.getOutputStream(), true);
        List<String> original = new ArrayList<>();
        int items = new Random().nextInt(25000) + 100;
        for (int i = 0; i < items; i++) {
            String s = UUID.randomUUID().toString();
            out.println(s);
            original.add(s);
            out.flush();
        }

        // disconnect
        out.close();
        producer.close();

        // reconnect and write again
        producer = new Socket("localhost", 12345);
        out = new PrintWriter(producer.getOutputStream(), true);
        items = new Random().nextInt(25000) + 100;
        for (int i = 0; i < items; i++) {
            String s = UUID.randomUUID().toString();
            out.println(s);
            original.add(s);
            out.flush();
        }

        List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> poll;
        do {
            poll = task.poll();
            records.addAll(poll);
        } while (poll.size() > 0);

        org.junit.Assert.assertEquals(original.size(), records.size());

        for (int i = 0; i < records.size(); i++) {
            String actual = ((Struct) records.get(i).value()).get("message").toString();
            String expected = original.get(i);
            org.junit.Assert.assertEquals(expected, actual);
        }


        out.close();
        producer.close();
    }

    @Test
    public void testMultipleProducers() throws Exception {
        List<Socket> producers = new ArrayList<>();
        List<PrintWriter> outputs = new ArrayList<>();
        int producersCount = new Random().nextInt(100);
        for (int i = 0; i < producersCount; i++) {
            producers.add(new Socket("localhost", 12345));
            outputs.add(new PrintWriter(producers.get(i).getOutputStream(), true));
        }
        List<String> original = new ArrayList<>();
        PrintWriter out;
        int items = new Random().nextInt(50000) + 100;
        for (int i = 0; i < items; i++) {
            out = outputs.get(new Random().nextInt(producersCount));
            String s = UUID.randomUUID().toString();
            out.println(s);
            original.add(s);
            out.flush();
        }

        List<SourceRecord> records = new ArrayList<>();
        List<SourceRecord> poll;
        do {
            poll = task.poll();
            records.addAll(poll);
        } while (poll.size() > 0);

        org.junit.Assert.assertEquals(original.size(), records.size());

//        for (int i = 0; i < records.size(); i++) {
//            String actual = ((Struct) records.get(i).value()).get("message").toString();
//            String expected = original.get(i);
//            org.junit.Assert.assertEquals(expected, actual);
//        }

        for (int i = 0; i < producersCount; i++) {
            outputs.get(i).close();
            producers.get(i).close();
        }
    }


    @After
    public void close() {
        task.stop();
    }
}
