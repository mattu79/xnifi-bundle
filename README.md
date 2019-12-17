数据集成平台XNIFI
==============
该项目的目标是建立易用、可靠的数据处理平台。

## 历史版本记录

* [xnifi-1.2.3 2019-07-12](#xnifi-1.2.3)
* [xnifi-1.2.2 2019-04-22](#xnifi-1.2.2)
* [xnifi-1.2.1 2019-02-13](#xnifi-1.2.1)
* [xnifi-1.2.0 2018-09-28](#xnifi-1.2.0)
* [xnifi-1.1.2 2018-09-10](#xnifi-1.1.2)
* [xnifi-1.1.1 2018-08-28](#xnifi-1.1.1)
* [xnifi-1.1.0 2018-08-03](#xnifi-1.1.0)
* [xnifi-1.0.0 2018-07-10](#xnifi-1.0.0)

### xnifi-1.2.3
1、Strings中增加toJson方法来将对象转换成JSON。

### xnifi-1.2.2
1. Dates中增加fromUtc方法来对UTC时间格式（这里特指0时区）进行处理。
1. Dates中增加toTimestamp方法来将Date对象转成timestamp字符串。

### xnifi-1.2.1
1. 增加GraphqlOnJson组件支持。

### xnifi-1.2.0
1. 增加对NEO4J图数据库的支持。
1. 增加ExecuteSqlOnJson组件，提供对SQL语句的支持。


### xnifi-1.1.2
1. 在SequenceGenerator中增加sequcenMaxValue的默认值和相关注释。

### xnifi-1.1.1
1. 去掉GraphqlParser中对category的默认值处理，改为使用Graphql事件上自带的category值。

### xnifi-1.1.0
1. 增加GraphqlEvent处理器。
1. 修正GenerateSequence中maxValue和format不匹配的错误。
1. 在RouteOnJson路由处理器上增加批处理大小选项。
1. 增加PartitionOnJson处理器对FlowFile中的JSON ROW进行分区处理。
1. 增加PutElasticsearchOnJson处理器以实现保存JSON ROW到ES中，配合PartitionOnJson使用。
1. 修正GraphqlParserUtils中GraphQL查询存在命令和未命名的情况。

### xnifi-1.0.0
1. 项目初始化，加入GenerateSequence时间序列处理器；
1. 加入RouteOnJson路由处理器；
1. 加入ScriptOnJson脚本处理器。




