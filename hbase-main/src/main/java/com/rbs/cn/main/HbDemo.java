package com.rbs.cn.main;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Table;
import utils.HBaseHelper;

import java.io.IOException;

/**
 * Created by fengtao.xue on 2018/1/26.
 */
public class HbDemo {

    public static void main(String[] args) throws IOException {

        Configuration conf = HBaseConfiguration.create();

        HBaseHelper helper = HBaseHelper.getHelper(conf);
        helper.dropTable("wctb");
        helper.createTable("wctb", "colfam1");

        Connection connection = ConnectionFactory.createConnection(conf);
        Table table = connection.getTable(TableName.valueOf("wctb"));

        //TODO something here
        //put, get, scan
        System.out.println("**************************************");
        System.out.println("**************************************");
        System.out.println("****** here is to do something *******");
        System.out.println("**************************************");
        System.out.println("***************************************");

        table.close();
        connection.close();
        helper.close();
    }
}
