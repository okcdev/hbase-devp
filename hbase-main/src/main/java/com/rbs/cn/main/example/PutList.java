/*
 * File: PutList.java
 * Created By: fengtao.xue@gausscode.com
 * Date: 2018-09-04
 */

package com.rbs.cn.main.example;

import com.rbs.cn.main.utils.HbaseEnv;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
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
public class PutList {
    static Logger logger = LoggerFactory.getLogger(PutList.class);

    public void run(Configuration conf, Connection connection) throws IOException {
        HBaseHelper helper = HBaseHelper.getHelper(conf);

        String tbName = "rbs";

        helper.dropTable(tbName);
        helper.createTable(tbName,"cf1", "cf2");
        //Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf(tbName));

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
        connection.close();
        helper.close();
    }
}
