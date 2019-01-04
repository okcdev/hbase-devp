/*
 * File: ClientBasicOpt.java
 * Created By: fengtao.xue@gausscode.com
 * Date: 2018-12-28
 */

package com.rbs.cn.main.example;

import javafx.scene.control.Tab;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.client.coprocessor.Batch;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.HBaseHelper;

import javax.swing.text.TabExpander;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.*;


/**
 * @author fengtao.xue
 */
public class ClientBasicOpt {
    static Logger logger = LoggerFactory.getLogger(ClientBasicOpt.class);

    /**
     * putExample
     * @param conf
     * @throws IOException
     */
    public void put(Configuration conf) throws IOException {
        // ^^ PutExample
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        // vv PutExample
        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable")); // co PutExample-2-NewTable Instantiate a new client.

        Put put = new Put(Bytes.toBytes("row1")); // co PutExample-3-NewPut Create put with specific row.

        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                Bytes.toBytes("val1")); // co PutExample-4-AddCol1 Add a column, whose name is "colfam1:qual1", to the put.
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                Bytes.toBytes("val2")); // co PutExample-4-AddCol2 Add another column, whose name is "colfam1:qual2", to the put.

        table.put(put); // co PutExample-5-DoPut Store row with column into the HBase table.
        table.close(); // co PutExample-6-DoPut Close table and connection instances to free resources.
        connection.close();
        // ^^ PutExample
        helper.close();
        // vv PutExample
    }

    /**
     * getExample
     * @param helper
     * @throws IOException
     */
    public void get(HBaseHelper helper) throws IOException {
        // ^^ GetExample
        //HBaseHelper helper = HBaseHelper.getHelper(conf);
        if (!helper.existsTable("testtable")) {
            helper.createTable("testtable", "colfam1");
        }
        //Connection connection = ConnectionFactory.createConnection(conf);
        // vv GetExample
        Table table = helper.getConnection().getTable(TableName.valueOf("testtable")); // co GetExample-2-NewTable Instantiate a new table reference.

        Get get = new Get(Bytes.toBytes("row1")); // co GetExample-3-NewGet Create get with specific row.

        get.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1")); // co GetExample-4-AddCol Add a column to the get.

        Result result = table.get(get); // co GetExample-5-DoGet Retrieve row with selected columns from HBase.

        byte[] val = result.getValue(Bytes.toBytes("colfam1"),
                Bytes.toBytes("qual1")); // co GetExample-6-GetValue Get a specific value for the given column.

        System.out.println("Value: " + Bytes.toString(val)); // co GetExample-7-Print Print out the value while converting it back.

        table.close(); // co GetExample-8-Close Close the table and connection instances to free resources.
        // ^^ GetExample
        helper.close();
    }

    /**
     * putListExample
     * @param helper
     * @throws IOException
     */
    public void putList(HBaseHelper helper) throws IOException {
        //HBaseHelper helper = HBaseHelper.getHelper(conf);

        String tbName = "rbs";

        helper.dropTable(tbName);
        helper.createTable(tbName,"cf1", "cf2");
        //Connection connection = ConnectionFactory.createConnection(conf);
        Table table = helper.getConnection().getTable(TableName.valueOf(tbName));

        List<Put> putList = new ArrayList<Put>();

        Put put1 = new Put(Bytes.toBytes("row1"));
        put1.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes("张三"));
        put1.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("sex"),Bytes.toBytes("男"));
        put1.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes(21));
        put1.addColumn(Bytes.toBytes("cf2"),Bytes.toBytes("weight"),Bytes.toBytes(57));
        put1.addColumn(Bytes.toBytes("cf2"),Bytes.toBytes("height"),Bytes.toBytes(175));
        put1.addColumn(Bytes.toBytes("cf2"),Bytes.toBytes("score"),Bytes.toBytes(65.32));
        putList.add(put1);

        Put put2 = new Put(Bytes.toBytes("row2"));
        put2.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("name"),Bytes.toBytes("李四"));
        put2.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("sex"),Bytes.toBytes("女"));
        put2.addColumn(Bytes.toBytes("cf1"),Bytes.toBytes("age"),Bytes.toBytes(23));
        put2.addColumn(Bytes.toBytes("cf2"),Bytes.toBytes("weight"),Bytes.toBytes(48));
        put2.addColumn(Bytes.toBytes("cf2"),Bytes.toBytes("height"),Bytes.toBytes(166));
        putList.add(put2);

        table.put(putList);
        table.close();
        helper.close();
    }


    /**
     * appendExample
     * @param conf
     * @throws IOException
     */
    public void append(Configuration conf) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("testtable");
        helper.createTable("testtable", 100, "colfam1", "colfam2");
        helper.put("testtable", new String[] {"row1"}, new String[]{"colfam1"},
                new String[] {"qual1","qual2"}, new long[] {1}, new String[] {"oldvalue1","oldvalue2"});
        logger.info("Befor append call...");
        helper.dump("testtable", new String[] {"row1"}, null, null);

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("testtable"));

        // vv AppendExample
        Append append = new Append(Bytes.toBytes("row1"));
        append.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"),
                Bytes.toBytes("newvalue"));
        append.add(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"),
                Bytes.toBytes("anothervalue"));

        table.append(append);
        // ^^ AppendExample
        System.out.println("After append call...");
        helper.dump("testtable", new String[]{"row1"}, null, null);
        table.close();
        connection.close();
        helper.close();
    }

    public void batchCallBack(HBaseHelper helper) throws IOException {
        //delete testtable if exist
        helper.dropTable("testtable");
        //create testtable
        helper.createTable("testtable", "colfam1", "colfam2");
        helper.put("testtable",
                new String[] {"row1"},
                new String[] {"colfam1"},
                new String[] {"qaul1","qaul2","qaul3"},
                new long[] {1,2,3},
                new String[] {"val1","val2","val3"});
        logger.info("Befor batch call...");
        helper.dump("testtable", new String[]{"row1", "row2"}, null, null);

        Table table = helper.getConnection().getTable(TableName.valueOf("testtable"));

        //create a batchCallbackExample
        List<Row> batch = new ArrayList<Row>();

        Put put = new Put(Bytes.toBytes("row2"));
        put.addColumn(Bytes.toBytes("colfam2"), Bytes.toBytes("qual1"), 4, Bytes.toBytes("val5"));
        batch.add(put);

        Get get1 = new Get(Bytes.toBytes("row1"));
        get1.addColumn(Bytes.toBytes("colfam1"),Bytes.toBytes("qual1"));
        batch.add(get1);

        Delete delete = new Delete(Bytes.toBytes("row1"));
        delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual2"));
        batch.add(delete);

        /*Get get2 = new Get(Bytes.toBytes("row2"));
        get2.addFamily(Bytes.toBytes("BOGUS"));
        batch.add(get2);*/

        Object[] results = new Object[batch.size()];
        try {
            table.batchCallback(batch, results, new Batch.Callback<Result>() {
                @Override
                public void update(byte[] region, byte[] row, Result result) {
                    System.out.println("Received callback for row[" +
                            Bytes.toString(row) + "] -> " + result);
                }
            });
        }catch (Exception e){
            logger.error("Error: {}\n{}", e.getMessage(), e.getStackTrace());
        }

        for (int i = 0; i < results.length; i++){
            logger.debug("Result:{},type = {}", i, results[i].getClass().getSimpleName() + results[i]);
        }

        table.close();
        logger.info("After batch call...");
        helper.dump("testtable", new String[] {"row1", "row2"}, null, null);
        helper.getConnection().close();
        helper.close();
    }

    public void batchSameRow(HBaseHelper helper) throws IOException {
        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");
        helper.put("testtable", "row1", "colfam1", "qual1", 1L, "vals");
        logger.info("Before batch call...");
        helper.dump("testtable", new String[] {"row1"}, null, null);

        Table table = helper.getConnection().getTable(TableName.valueOf("testtable"));

        List<Row> batch = new ArrayList<Row>();

        Put put = new Put(Bytes.toBytes("row1"));
        put.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), 2L, Bytes.toBytes("val2"));
        batch.add(put);

        Get get1 = new Get(Bytes.toBytes("row1"));
        get1.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        batch.add(get1);

        Delete delete = new Delete(Bytes.toBytes("row1"));
        delete.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"), 3L);
        batch.add(delete);

        Get get2 = new Get(Bytes.toBytes("row1"));
        get2.addColumn(Bytes.toBytes("colfam1"), Bytes.toBytes("qual1"));
        batch.add(get2);

        Object[] results = new Object[batch.size()];
        try {
            table.batch(batch, results);
        } catch (InterruptedException e) {
            logger.error("Error: {}\n{}", e.getMessage(), e.getStackTrace());
        }

        for (int i = 0; i < results.length; i++) {
            System.out.println("Result[" + i + "]: type = " +
                    results[i].getClass().getSimpleName() + "; " + results[i]);
        }

        table.close();
        logger.info("After batch call");
        helper.dump("testtable", new String[] {"row1"}, null, null);
        helper.getConnection().close();
        helper.close();
    }

    public void bufferedMutator(HBaseHelper helper) throws IOException {
        int POOL_SIZE = 10;
        int TASK_COUNT = 100;
        TableName TABLE = TableName.valueOf("testtable");
        final byte[] FAMILY = Bytes.toBytes("colfam1");

        helper.dropTable("testtable");
        helper.createTable("testtable", "colfam1");

        BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator bufferedMutator) throws RetriesExhaustedWithDetailsException {
                for (int i = 0; i < e.getNumExceptions(); i++){
                    logger.info("Faild to send put:{}", e.getRow(i));
                }
            }
        };

        BufferedMutatorParams params = new BufferedMutatorParams(TABLE).listener(listener);

        try {
            final BufferedMutator mutator = helper.getConnection().getBufferedMutator(params);

            ExecutorService workerPool = Executors.newFixedThreadPool(POOL_SIZE);
            List<Future<Void>> futures = new ArrayList<Future<Void>>(TASK_COUNT);

            for (int i = 0; i < TASK_COUNT; i++){
                futures.add(workerPool.submit(new Callable<Void>() {
                    @Override
                    public Void call() throws Exception {
                        Put p = new Put(Bytes.toBytes("row1"));
                        p.addColumn(FAMILY, Bytes.toBytes("qual1"), Bytes.toBytes("val1"));
                        mutator.mutate(p);
                        return null;
                    }
                }));
            }

            for (Future<Void> f : futures){
                f.get(5, TimeUnit.MINUTES);
            }
            workerPool.shutdown();
        }catch (IOException e){

        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        } catch (TimeoutException e) {
            e.printStackTrace();
        }
    }
}
