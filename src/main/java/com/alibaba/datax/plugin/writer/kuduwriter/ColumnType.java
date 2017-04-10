package com.alibaba.datax.plugin.writer.kuduwriter;


import com.alibaba.datax.common.exception.DataXException;
import org.apache.commons.lang3.StringUtils;

import java.util.Arrays;


import static com.alibaba.datax.plugin.writer.kuduwriter.kuduWriterErrorCode.ILLEGAL_VALUE;

/**
 * Created by NGames on 2017/2/28.
 */
public enum ColumnType {
    STRING("string"),
    BOOLEAN("boolean"),
    SHORT("short"),
    INT("int"),
    LONG("long"),
    FLOAT("float"),
    DOUBLE("double")
    ;

    private String typeName;

    ColumnType(String typeName) {
        this.typeName = typeName;
    }

    public static ColumnType getByTypeName(String typeName) {
        if(StringUtils.isBlank(typeName)){
            throw DataXException.asDataXException(ILLEGAL_VALUE,
                    String.format("kuduwriter 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
        }
        for (ColumnType columnType : values()) {
            if (StringUtils.equalsIgnoreCase(columnType.typeName, typeName.trim())) {
                return columnType;
            }
        }

        throw DataXException.asDataXException(ILLEGAL_VALUE,
                String.format("kuduwriter 不支持该类型:%s, 目前支持的类型是:%s", typeName, Arrays.asList(values())));
    }

    @Override
    public String toString() {
        return this.typeName;
    }
}

