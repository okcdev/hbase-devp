/*
 * File: ClientBasicOpt.java
 * Created By: fengtao.xue@gausscode.com
 * Date: 2018-12-28
 */

package com.rbs.cn.main.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.*;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.HBaseHelper;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;


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
}
