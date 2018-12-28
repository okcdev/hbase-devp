/*
 * File: HbaseGet.java
 * Created By: fengtao.xue@gausscode.com
 * Date: 2018-09-03
 */

package com.rbs.cn.main.example;

import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.HBaseHelper;

import java.io.IOException;


/**
 * @author fengtao.xue
 */
public class HbaseGet {
    static Logger logger = LoggerFactory.getLogger(HbaseGet.class);

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
}
