/*
 * File: Main.java
 * Created By: fengtao.xue@gausscode.com
 * Date: 2018-09-03
 */

package com.rbs.cn.main;

import com.rbs.cn.main.example.HbaseGet;
import com.rbs.cn.main.example.HbasePut;
import com.rbs.cn.main.utils.HbaseConf;
import org.apache.hadoop.conf.Configuration;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;


/**
 * @author fengtao.xue
 */
public class Main {
    static Logger logger = LoggerFactory.getLogger(Main.class);

    public static void main(String[] args) throws IOException {

        Configuration conf = HbaseConf.init();

        /*HbasePut hbasePut = new HbasePut();
        hbasePut.put(conf);*/

        HbaseGet hbaseGet = new HbaseGet();
        hbaseGet.get(conf);

    }
}
