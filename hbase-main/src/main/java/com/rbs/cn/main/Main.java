/*
 * File: Main.java
 * Created By: fengtao.xue@gausscode.com
 * Date: 2018-09-03
 */

package com.rbs.cn.main;

import com.rbs.cn.main.example.HbaseGet;
import com.rbs.cn.main.example.PutList;
import com.rbs.cn.main.utils.HbaseEnv;
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

        logger.info("*************init HbaseEnv******************");
        if (!HbaseEnv.initFlag){
            HbaseEnv.init();
        }
        logger.info("**********init HbaseEnv successfully********");

        /*HbasePut hbasePut = new HbasePut();
        hbasePut.put(conf);*/

        logger.info("*****************do get start **************");
        HbaseGet hbaseGet = new HbaseGet();
        hbaseGet.get(HbaseEnv.helper);
        logger.info("****************do get successfully*********");

        /*logger.info("*****************do PutList start **************");
        PutList putList = new PutList();
        putList.run(HbaseEnv.helper);
        logger.info("****************do PutList successfully*********");*/
    }
}
