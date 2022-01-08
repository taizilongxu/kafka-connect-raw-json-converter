# kafka-connect-raw-json-converter

## 解决问题
正常 JSON 通过 kafka-connect hdfs sink 写入 HDFS 有两种方式:
1. 纯 JSON 格式, 只能单条的写入 JSON, 无法压缩
2. 带 Schema 的 JSON 格式 `{"schema": xx, "payload": xx}`, 可以写入 AVRO 等带压缩存储

此 Converter 实现纯 JSON 格式外加 Schema Registry 指定 JSON 格式写入 AVRO 等压缩存储

## 准备

1. 此 `mvn package` 后的 Jar 包放入类路径中
2. 需要一个 Schema Registry 实例
3. 修改配置如下

```yaml
key.converter=org.apache.kafka.connect.json.JsonConverter  # 一般 Kafka key 为空值
key.converter.schemas.enable=false
value.converter=com.github.hackerxu.kafka.connect.JsonToSchemaConverter
value.converter.schema.registry.url=http://localhost:8081  # Schema Registry 地址
value.converter.schema.id=1  # 需要使用的 schema id
value.converter.schemas.enable=true  # 开启 schema
```

指定一下写入 HDFS 格式

```yaml
format.class=io.confluent.connect.hdfs.parquet.ParquetFormat
parquet.codec=snappy
```

## 问题

比如 Filebeat 有些特殊字符`@`, 需要在用 transform 进行转换,
而且如果是 Parquet 模式, 需要对 nested 格式进行 flatten

```yaml
transforms=RenameField,flatten
transforms.RenameField.type=org.apache.kafka.connect.transforms.ReplaceField$Value
transforms.RenameField.renames=@timestamp:timestamp,@metadata:metadata
transforms.flatten.type=org.apache.kafka.connect.transforms.Flatten$Value
transforms.flatten.delimiter=_
```

