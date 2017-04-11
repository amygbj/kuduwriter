# kuduwriter

-----

>  kuduwriter是datax的插件，提供了将数据写入apache kudu数据库的功能

-----

### json模板说明：

```

  "writer": {
                    "name": "kuduwriter",
                                       "parameter": {
                                                  "table": "table",
                                                 "master": "ip",
                                                "primaryKey": [
                                                {
                                                "index":0,
                                                "name": "key",
                                                "type":"string"
                                                }
                                                ],
                                                  "column": [
                                                    {
                                                        "index":1,
                                                        "name": "col1",
                                                        "type": "int"
                                                }
										],
                                         "encoding": "utf-8"
                                        }
                }
            }
        ]
    }
}


```

### 参数解释

table:
kudu表名字，必选参数

-----

master：
kudumaster,必选参数

-----

primaryKey：
kudu表的主键列，可以为多个，必选参数

-----

column：
kudu表的列名,必选参数

-----

encoding:
编码，非必选参数


