/*
 * File: HbaseConf.java
 * Created By: fengtao.xue@gausscode.com
 * Date: 2018-09-02
 */

package com.rbs.cn.main.utils;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;


/**
 * @author fengtao.xue
 */
public class HbaseConf {
    static Logger logger = LoggerFactory.getLogger(HbaseConf.class);

    static Configuration conf = null;

    public static Configuration init(){
        conf = HBaseConfiguration.create();
        conf.set("hbase.zookeeper.quorum", "localhost");
        conf.set("hbase.zookeeper.property.clientPort", "2180");
        //conf.set("hbase.master", "localhost:9001");
        return conf;
    }
}
