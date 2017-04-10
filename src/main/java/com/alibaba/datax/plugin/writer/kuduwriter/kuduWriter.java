package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.element.Column;
import com.alibaba.fastjson.JSONArray;
import com.alibaba.fastjson.JSONObject;
import org.apache.kudu.client.*;
import com.alibaba.datax.common.element.Record;
import com.alibaba.datax.common.exception.DataXException;
import com.alibaba.datax.common.plugin.RecordReceiver;
import com.alibaba.datax.common.spi.Writer;
import com.alibaba.datax.common.util.Configuration;
import org.apache.kudu.client.KuduClient;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.List;
import com.alibaba.fastjson.JSON;

import static com.alibaba.datax.plugin.writer.kuduwriter.kuduWriterErrorCode.*;

/**
 * kuduwriter的主操作类
 * Created by NGames on 2017/2/28.
 */
public class kuduWriter extends Writer {

    public static final class Job extends Writer.Job {

        private final Logger log = LoggerFactory.getLogger(this.getClass());

        private Configuration originalConfig;
        private String tableName;  //kudu表名
        private String column;  //kudu的列名字
        private String primaryColumn; //主键
        private String encoding;  //编码
        private String kudu_master; //kudu.master
        @SuppressWarnings("unchecked")
        @Override
        public void init() {
            this.originalConfig = super.getPluginJobConf();
            log.info(originalConfig.toString());
            //在Job中判断配置项格式合法性,以尽早发现配置问题
            //获取需要写入的kudutable
            tableName=originalConfig.getNecessaryValue(Key.KEY_KUDU_TABLE,KUDU_ERROR_TABLE);
            log.info("table:"+tableName);
            //获取需要写入的columes
            column=originalConfig.getNecessaryValue(Key.KEY_KUDU_COLUMN,KUDU_ERROR_COLUMNS);
            log.info("column:"+column);
            //获取需要写入的primaryColumn
            primaryColumn=originalConfig.getNecessaryValue(Key.KET_KUDU_PRIMARY,KUDU_ERROR_PRIMARY_KEY);
            log.info("primaryColumn:"+primaryColumn);
            //检查kudu.master
            kudu_master=originalConfig.getNecessaryValue(Key.KEY_KUDU_MASTER,KUDU_ERROR_MASTER);
            log.info("master:"+kudu_master);
            encoding = originalConfig.getUnnecessaryValue(Key.KEY_KUDU_ENCODE, "utf-8", KUDU_ERROR_CODEING);
        }

        @Override
        public void destroy() {

        }

        @Override
        public List<Configuration> split(int mandatoryNumber) {
            List<Configuration> list = new ArrayList<Configuration>();
            for (int i = 0; i < mandatoryNumber; i++) {
                list.add(originalConfig.clone());
            }
            return list;
        }

    } //public static final class Job extends Writer.Job


    public static class Task extends Writer.Task {
        private final Logger log = LoggerFactory.getLogger(this.getClass());

        private Configuration sliceConfig;
        private int rowkeyIndex = 0;
        private String tableName;  //kudu表名
        private String column;  //所有列
        private KuduClient kuduClient;  //kuduclient
        private String encoding;  //编码格式
        private List<JSONObject> kuduColumnsList = new ArrayList<JSONObject>(); //kudu列集合 元素的解析后的集合
        private String  kudu_master;  //kudu_master
        private String primaryColumn; //主键
        private  KuduSession session;  //kudu-session
        private  KuduTable kuduTable;  //kudu-table
        private List<JSONObject>  columnMap; //column
        @SuppressWarnings("unchecked")
        @Override
        public void init() {
            this.sliceConfig = super.getPluginJobConf();
            log.info(sliceConfig.toString());
            //获取需要写入的kudutable  table节点
            tableName=sliceConfig.getNecessaryValue(Key.KEY_KUDU_TABLE,KUDU_ERROR_TABLE);
            //获取需要写入的columes  column节点
            column=sliceConfig.getNecessaryValue(Key.KEY_KUDU_COLUMN,KUDU_ERROR_COLUMNS);
            log.info("columns :"+column);
            //获取需要写入的primaryKey  primaryKey 节点
            primaryColumn=sliceConfig.getNecessaryValue(Key.KET_KUDU_PRIMARY,KUDU_ERROR_PRIMARY_KEY);
            //获取编码 encoding节点
            encoding = sliceConfig.getUnnecessaryValue(Key.KEY_KUDU_ENCODE, "utf-8", KUDU_ERROR_CODEING);
            //获取kudu.master
            kudu_master=sliceConfig.getNecessaryValue(Key.KEY_KUDU_MASTER,KUDU_ERROR_MASTER);

            //添加主键
            //primaryKey_idex [{"index":0,"name":"g_uid","type":"string"}]
            JSONArray array=JSON.parseArray(primaryColumn);
            if (array.size()==0){  //没有主键配置
                throw DataXException.asDataXException(KUDU_ERROR_PRIMARY_KEY,KUDU_ERROR_PRIMARY_KEY.getDescription());
            }
            else{
                for (Object obj : array){
                    JSONObject obj2=JSON.parseObject(obj.toString());
                    if (!obj2.containsKey("index")) {
                        throw DataXException.asDataXException(KUDU_ERROR_PRIMARY_KEY, KUDU_ERROR_PRIMARY_KEY.getDescription());
                    }
                    else if (!obj2.containsKey("name")) {
                        throw DataXException.asDataXException(KUDU_ERROR_PRIMARY_KEY, KUDU_ERROR_PRIMARY_KEY.getDescription());
                    }
                    else if (!obj2.containsKey("name")) {
                        throw DataXException.asDataXException(KUDU_ERROR_PRIMARY_KEY, KUDU_ERROR_PRIMARY_KEY.getDescription());
                    }
                    else {
                        kuduColumnsList.add(obj2.getInteger("index"),obj2);  //添加主键列和主键列的索引号
                    }
                }
            }

            //column map
            columnMap = (List<JSONObject>)sliceConfig.get(Key.KEY_KUDU_COLUMN, List.class);

            //添加列
            for(JSONObject hColumn : columnMap) {
                if (!hColumn.containsKey("index")) {
                    throw DataXException.asDataXException(KUDU_ERROR_CONF_COLUMNS, KUDU_ERROR_CONF_COLUMNS.getDescription());
                }
                else if (!hColumn.containsKey("name")) {
                    throw DataXException.asDataXException(KUDU_ERROR_CONF_COLUMNS, KUDU_ERROR_CONF_COLUMNS.getDescription());
                }
                else if (!hColumn.containsKey("type")) {
                    throw DataXException.asDataXException(KUDU_ERROR_CONF_COLUMNS, KUDU_ERROR_CONF_COLUMNS.getDescription());
                }
                else {
                    kuduColumnsList.add(hColumn.getInteger("index"),hColumn);  //添加列
                }
            }


            //初始化kuduclient
            log.info("初始化kuduclient");
            kuduClient = new KuduClient.KuduClientBuilder(kudu_master).build();
            session = kuduClient.newSession();
            try {
                kuduTable= kuduClient.openTable(tableName);

            } catch (KuduException e) {
                e.printStackTrace();
            }
        }

        @Override
        public void destroy() {
            if(kuduClient != null) {
                try {
                    //关闭kuduclient
                    kuduClient.shutdown();
                    kuduClient.close();
                } catch (Exception e) {
                    log.error("close table "+tableName+" failed.", e);
                    throw DataXException.asDataXException(KUDU_RUNNING_ERROR, KUDU_RUNNING_ERROR.getDescription());
                }
            }
        }

        /**
         * 写入kudu表
         * @param lineReceiver
         */
        @Override
        public void startWrite(RecordReceiver lineReceiver) {
            int ok = 0;
            int count = 0;
            Record record = null;

            //除了第一列rowkey主键外必须还有其他字段
            if(kuduColumnsList != null && kuduColumnsList.size() > 1) {
                List<PartialRow> cells = new ArrayList<PartialRow>();  //kudu_rows

                //开始读取列
                while((record = lineReceiver.getFromReader()) != null) {

                    if(kuduColumnsList.size() != record.getColumnNumber()) {
                        throw DataXException.asDataXException(ILLEGAL_VALUES_ERROR, ILLEGAL_VALUES_ERROR.getDescription() +
                                "读出字段个数:" + record.getColumnNumber() + " " + "配置字段个数:" + kuduColumnsList.size());
                    }
                    //获取primarykey
                    String primarykey = record.getColumn(this.rowkeyIndex).asString();
                    if(primarykey != null && !"".equals(primarykey)) {
                        //写入一行数据
                        Insert insert=kuduTable.newInsert();
                        PartialRow row = insert.getRow();
                        for(int i = 0; i < kuduColumnsList.size(); i++) {
                            JSONObject col = kuduColumnsList.get(i);  //列名的说明
                            String columnName=col.getString("name");  //列字段名
                            ColumnType columnType=ColumnType.getByTypeName(col.getString("type"));  //列类型
                            Column column=record.getColumn(col.getInteger("index"));  //获取对应的数据列
                            if(column.getRawData() != null) { //列数据不为空
                                switch (columnType) {
                                    case INT:
                                        row.addInt(columnName,Integer.parseInt(column.getRawData().toString()));
                                        break;
                                    case FLOAT:
                                        row.addFloat(columnName,Float.parseFloat(column.getRawData().toString()));
                                        break;
                                    case STRING:
                                        row.addString(columnName,column.getRawData().toString());
                                        break;
                                    case DOUBLE:
                                        row.addDouble(columnName,Double.parseDouble(column.getRawData().toString()));
                                        break;
                                    case LONG:
                                        row.addLong(columnName,Long.parseLong(column.getRawData().toString()));
                                        break;
                                    case BOOLEAN:
                                        row.addBoolean(columnName,Boolean.getBoolean(column.getRawData().toString()));
                                        break;
                                    case SHORT:
                                        row.addShort(columnName,Short.parseShort(column.getRawData().toString()));
                                        break;
                                    default:
                                        throw DataXException
                                                .asDataXException(ILLEGAL_VALUE,
                                                        String.format(
                                                                "您的配置文件中的列配置信息有误. 因为kudu不支持写入这种字段类型. 字段名:[%s], 字段类型:[%d]. 请修改表中该字段的类型或者不同步该字段.",
                                                                columnName,
                                                                col.getString("type")));
                                }
                            }
                        }
                        //add one row
                        count++;
                        ok++;
                        try {
                            session.apply(insert);
                        } catch (KuduException e) {
                            e.printStackTrace();
                        }
                    } //if(rowKey != null && !"".equals(rowKey))
                    else {
                        //收集脏数据
                        super.getTaskPluginCollector().collectDirtyRecord(record, "primarykey字段为空");
                    }
                } //while((record = lineReceiver.getFromReader()) != null)
                log.info(ok + " rows are successfully inserted into the table " + this.tableName);
            } //if(hbaseColumnsList != null && hbaseColumnsList.size() > 1)

        } //public void startWrite(RecordReceiver lineReceiver)

    } //public static class Task extends Writer.Task

}
