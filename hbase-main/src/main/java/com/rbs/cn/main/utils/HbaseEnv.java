/*
 * File: HbaseEnv.java
 * Created By: fengtao.xue@gausscode.com
 * Date: 2018-09-02
 */

package com.rbs.cn.main.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * @author fengtao.xue
 */
public class HbaseEnv {
    static Logger logger = LoggerFactory.getLogger(HbaseEnv.class);

    public static Configuration conf = null;
    public static Connection connection = null;
    public static boolean initFlag = false;

    public static boolean init() throws IOException {
        if (!initFlag){
            conf = HBaseConfiguration.create();
            conf.set("hbase.zookeeper.quorum", "localhost");
            conf.set("hbase.zookeeper.property.clientPort", "2180");
            //conf.set("hbase.master", "localhost:9001");
            connection = ConnectionFactory.createConnection(conf);
            initFlag = true;
        }
        return initFlag;
    }
}
