/*
 * File: AppendExample.java
 * Created By: fengtao.xue@gausscode.com
 * Date: 2018-12-28
 */

package com.rbs.cn.main.example;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.client.Append;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import utils.HBaseHelper;

import java.io.IOException;


/**
 * @author fengtao.xue
 */
public class AppendExample {
    static Logger logger = LoggerFactory.getLogger(Append.class);

    public void run(Configuration conf) throws IOException {

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
