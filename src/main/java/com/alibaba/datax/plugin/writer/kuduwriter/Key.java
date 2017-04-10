package com.alibaba.datax.plugin.writer.kuduwriter;

/**
 * Created by NGames on 2017/2/28.
 * kudu表的配置信息
 */
public class Key {
    public static final String KEY_KUDU_TABLE = "table";  //配置中的kudu表名字
    public static final String KEY_KUDU_COLUMN = "column";  //kudu表名字对应的读取列
    public static final String KEY_KUDU_ENCODE = "encoding";
    public static final  String KEY_KUDU_MASTER="master";  //配置中的kudu-master
    public static final  String KET_KUDU_PRIMARY="primaryKey";  //kudu表的主键
}
