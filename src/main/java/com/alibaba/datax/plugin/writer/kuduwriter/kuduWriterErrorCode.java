package com.alibaba.datax.plugin.writer.kuduwriter;

import com.alibaba.datax.common.spi.ErrorCode;

/**
 * Created by NGames on 2017/2/28.
 */
public enum kuduWriterErrorCode  implements ErrorCode {

    KUDU_ERROR_TABLE("KUDU_ERROR_TABLE","请检查kudu表名！！"),
    KUDU_ERROR_COLUMNS("KUDU_ERROR_COLUMNS","请检查kudu列名！！"),
    KUDU_ERROR_MASTER("KUDU_ERROR_MASTER","请检查kudu_master！！"),
    KUDU_ERROR_CODEING("KUDU_ERROR_CODEING","kudu表数据编码没有配置"),
    KUDU_ERROR_CONF_COLUMNS("KUDU_ERROR_CONF_COLUMNS","kudu表字段没有配置或配置不正确"),
    KUDU_ERROR_PRIMARY_KEY("KUDU_ERROR_PRIMARY_KEY","kudu表主键没有配置"),
    ILLEGAL_VALUES_ERROR("ILLEGAL_VALUES_ERROR","列名配置不匹配！"),
    KUDU_RUNNING_ERROR("KUDU_RUNNING_ERROR", "kudu-client运行时异常"),
    ILLEGAL_VALUE("ILLEGAL_VALUE","列类型配置错误！");

    kuduWriterErrorCode(String code, String desc) {
        this.code = code;
        this.desc = desc;
    }


    private String code;
    private String desc;
    @Override
    public String getCode() {
        return this.code;
    }

    @Override
    public String getDescription() {
        return this.desc;
    }
}
