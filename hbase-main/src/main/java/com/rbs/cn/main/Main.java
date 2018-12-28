/*
 * File: Main.java
 * Created By: fengtao.xue@gausscode.com
 * Date: 2018-09-03
 */

package com.rbs.cn.main;

import com.rbs.cn.main.example.ClientBasicOpt;
import com.rbs.cn.main.utils.HbaseEnv;
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

        ClientBasicOpt clientBasicOpt= new ClientBasicOpt();

        /*logger.info("******************** do put start ********************");
        clientBasicOpt.put(HbaseEnv.conf);
        logger.info("******************** do put successfully ***************");*/

        /*logger.info("******************** do get start ********************");
        clientBasicOpt.get(HbaseEnv.helper);
        logger.info("********************** do get successfully *************");*/

        /*logger.info("******************** do PutList start ****************");
        clientBasicOpt.putList(HbaseEnv.helper);
        logger.info("********************* do PutList successfully **********");*/

        /*logger.info("******************** do append start ****************");
        clientBasicOpt.append(HbaseEnv.conf);
        logger.info("********************* do append successfully **********");*/

        logger.info("******************** do batchCallBack start ****************");
        clientBasicOpt.batchCallBack(HbaseEnv.helper);
        logger.info("********************* do batchCallBack successfully **********");
    }
}
