/* Copyright (c) 2022 com.github.anyzm. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
package io.github.anyzm.graph.ocean.domain;

import java.util.Map;

/**
 * Description  GraphQuery is used for
 *
 * @author Anyzm
 * Date  2021/8/10 - 14:18
 * @version 1.0.0
 */
public interface GraphQuery {

    /**
     * 构造sql
     *
     * @return sql
     */
    String buildSql();

    /**
     * 连接两个查询片段
     *
     * @param graphQuery 另一个查询API
     * @return 查询API
     */
    GraphQuery connectAdd(GraphQuery graphQuery);

    /**
     * limit
     *
     * @param size 条数
     * @return 查询API
     */
    GraphQuery limit(int size);

    /**
     * limit
     *
     * @param offset 偏移量
     * @param size   条数
     * @return 查询API
     */
    GraphQuery limit(int offset, int size);


    /**
     * 去重
     *
     * @return 查询API
     */
    GraphQuery distinct();

    /**
     * 添加yield关键字
     *
     * @return 查询API
     */
    GraphQuery yield();


    /**
     * 查询哪个标签的哪些属性
     *
     * @param clazz  类类型
     * @param fields 字段
     * @return 查询API
     */
    GraphQuery yield(Class clazz, String... fields);

    /**
     * 查询哪个标签的哪些属性
     *
     * @param symbol 前缀符号
     * @param clazz  类类型
     * @param fields 字段
     * @return 查询API
     */
    GraphQuery yield(String symbol, Class clazz, String... fields);


    /**
     * 查询哪些属性
     *
     * @param fields 字段
     * @return 查询API
     */
    GraphQuery yield(String... fields);


    /**
     * 查询哪些属性
     *
     * @param fieldAlias 字段与别名映射
     * @return 查询API
     */
    GraphQuery yield(Map<String, String> fieldAlias);


    /**
     * 查询哪个标签的哪些属性
     *
     * @param clazz  类类型
     * @param fields 字段
     * @return 查询API
     */
    GraphQuery yieldDistinct(Class clazz, String... fields);


    /**
     * 查询哪个标签的哪些属性
     *
     * @param prefix 前缀符号
     * @param clazz  类类型
     * @param fields 字段
     * @return 查询API
     */
    GraphQuery yieldDistinct(String prefix, Class clazz, String... fields);


    /**
     * 查询哪些属性
     *
     * @param fields 字段
     * @return 查询API
     */
    GraphQuery yieldDistinct(String... fields);


    /**
     * 查询哪些属性
     *
     * @param fieldAlias 字段与别名映射
     * @return 查询API
     */
    GraphQuery yieldDistinct(Map<String, String> fieldAlias);


    /**
     * 管道分隔符
     *
     * @return 查询API
     */
    GraphQuery pipe();

    /**
     * 连接两个查询结果，查询列和列名需要一一对应
     *
     * @param graphQuery
     * @return
     */
    GraphQuery unionAll(GraphQuery graphQuery);

    /**
     * 连接两个查询结果，查询列和列名需要一一对应，会去除重复结果
     *
     * @param graphQuery 查询API
     * @return 查询API
     */
    GraphQuery union(GraphQuery graphQuery);

    /**
     * 根据某些属性分组查询
     *
     * @param clazz  类类型
     * @param fields 字段
     * @return 查询API
     */
    GraphQuery groupBy(Class clazz, String... fields);


    /**
     * 根据某些属性分组查询
     *
     * @param fields 字段
     * @return 查询API
     */
    GraphQuery groupBy(String... fields);

    /**
     * count(*)
     *
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery countComma(String alias);


    /**
     * 对某字段计数并取别名
     *
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery countComma(String field, String alias);

    /**
     * 计算属性
     *
     * @param clazz 类类型
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery countComma(Class clazz, String field, String alias);

    /**
     * count条件表达式
     *
     * @param graphExpression 表达式
     * @param alias           别名
     * @return 查询API
     */
    GraphQuery countComma(GraphExpression graphExpression, String alias);


    /**
     * 对某字段计数并取别名
     *
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery count(String field, String alias);


    /**
     * 对某些字段计数并取别名
     *
     * @param fieldAlias 字段与别名map
     * @return 查询API
     */
    GraphQuery count(Map<String, String> fieldAlias);

    /**
     * count(*)
     *
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery count(String alias);

    /**
     * 计算属性
     *
     * @param clazz 类类型
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery count(Class clazz, String field, String alias);


    /**
     * count条件表达式
     *
     * @param graphExpression 表达式
     * @param alias           别名
     * @return 查询API
     */
    GraphQuery count(GraphExpression graphExpression, String alias);

    /**
     * avg条件表达式
     *
     * @param graphExpression 表达式
     * @param alias           别名
     * @return 查询API
     */
    GraphQuery avg(GraphExpression graphExpression, String alias);

    /**
     * 某个字段的平均值
     *
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery avg(String field, String alias);

    /**
     * 某个字段的平均值
     *
     * @param clazz 类类型
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery avg(Class clazz, String field, String alias);

    /**
     * avgComma条件表达式
     *
     * @param graphExpression 表达式
     * @param alias           别名
     * @return 查询API
     */
    GraphQuery avgComma(GraphExpression graphExpression, String alias);

    /**
     * 某个字段的平均值，并且逗号分割
     *
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery avgComma(String field, String alias);

    /**
     * 某个字段的平均值，并且逗号分割
     *
     * @param clazz 类类型
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery avgComma(Class clazz, String field, String alias);

    /**
     * sum条件表达式
     *
     * @param graphExpression 表达式
     * @param alias           别名
     * @return 查询API
     */
    GraphQuery sum(GraphExpression graphExpression, String alias);

    /**
     * 某个字段的求和
     *
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery sum(String field, String alias);

    /**
     * 某个字段的求和
     *
     * @param clazz 类类型
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery sum(Class clazz, String field, String alias);

    /**
     * sumComma条件表达式
     *
     * @param graphExpression 表达式
     * @param alias           别名
     * @return 查询API
     */
    GraphQuery sumComma(GraphExpression graphExpression, String alias);

    /**
     * 某个字段的求和，并且逗号分割
     *
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery sumComma(String field, String alias);

    /**
     * 某个字段的求和，并且逗号分割
     *
     * @param clazz 类类型
     * @param field 字段
     * @param alias 别名
     * @return 查询API
     */
    GraphQuery sumComma(Class clazz, String field, String alias);

    /**
     * 逗号分隔
     *
     * @return 查询API
     */
    GraphQuery comma();

    /**
     * 条件过滤
     *
     * @param graphCondition 表达式
     * @return 查询API
     */
    GraphQuery where(GraphCondition graphCondition);
}
