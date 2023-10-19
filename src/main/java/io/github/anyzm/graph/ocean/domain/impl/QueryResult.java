/* Copyright (c) 2022 com.github.anyzm. All rights reserved.
 *
 * This source code is licensed under Apache 2.0 License,
 * attached with Common Clause Condition 1.0, found in the LICENSES directory.
 */
package io.github.anyzm.graph.ocean.domain.impl;

import com.vesoft.nebula.client.graph.data.Node;
import com.vesoft.nebula.client.graph.data.ResultSet;
import com.vesoft.nebula.client.graph.data.ValueWrapper;
import io.github.anyzm.graph.ocean.annotation.GraphProperty;
import io.github.anyzm.graph.ocean.common.utils.FieldUtils;
import io.github.anyzm.graph.ocean.domain.GraphLabel;
import io.github.anyzm.graph.ocean.enums.GraphDataTypeEnum;
import io.github.anyzm.graph.ocean.enums.GraphPropertyTypeEnum;
import io.github.anyzm.graph.ocean.exception.NebulaException;
import io.vavr.control.Try;
import lombok.Getter;
import lombok.ToString;

import java.io.Serializable;
import java.io.UnsupportedEncodingException;
import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.function.BiFunction;
import java.util.function.Supplier;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * @author Anyzm
 * @version 1.0.0
 * description QueryResult is used for
 * date 2020/3/27 - 10:13
 * @update chenrui
 * @date 2020/08/30
 */
@Getter
@ToString
public class QueryResult implements Iterable<ResultSet.Record>, Serializable {

    private List<ResultSet.Record> data = new ArrayList<>();

    private final Map<GraphDataTypeEnum, BiFunction<GraphLabel, ValueWrapper, Object>> valueTransformers;

    private static final String VERTEX = "v";

    public QueryResult() {
    }

    public QueryResult(List<ResultSet.Record> data) {
        this.data = data;
    }

    public int size() {
        return this.data.size();
    }

    public boolean isEmpty() {
        return this.size() == 0;
    }

    public boolean isNotEmpty() {
        return this.size() != 0;
    }

    @Override
    public Iterator<ResultSet.Record> iterator() {
        return this.data.iterator();
    }

    public Stream<ResultSet.Record> stream() {
        return this.data.stream();
    }

    public QueryResult mergeQueryResult(QueryResult queryResult) {
        return new QueryResult(
                Stream.concat(this.data.stream(), queryResult.data.stream())
                        .collect(Collectors.toList())
        );
    }

    public <T> List<T> getEntities(GraphLabel graphLabel, Class<T> clazz) {
        return this.data.stream()
                .map(record -> Try.of(() -> parseResult(record, graphLabel, clazz)))
                .filter(Try::isSuccess)
                .map(Try::get)
                .collect(Collectors.toList());
    }

    public Map<Class<?>, List<Object>> getEntities(Map<String, GraphLabel> labelMap) throws IllegalAccessException, InstantiationException, UnsupportedEncodingException {
        return Optional.ofNullable(this.data)
                .orElseGet(Collections::emptyList)
                .stream()
                .map(
                        record ->
                                Try.of(() -> parseResult(record, labelMap))
                                        .getOrElseThrow(NebulaException::new))
                .flatMap(Collection::stream)
                .collect(Collectors.groupingBy(Object::getClass));
    }

    private <T> T parseResult(ResultSet.Record record, GraphLabel graphLabel, Class<T> clazz) throws Exception {
        T obj = clazz.newInstance();
        FieldUtils.listFields(clazz)
                .forEach(field -> setFieldValueFromRecord(record, graphLabel, obj, field));
        return obj;
    }

    private List<Object> parseResult(ResultSet.Record record, Map<String, GraphLabel> labelMap) {
        return Optional.ofNullable(record.get(VERTEX))
                .filter(valueWrapper -> !valueWrapper.isNull())
                .filter(ValueWrapper::isVertex)
                .flatMap(
                        valueWrapper ->
                                Try.of(() -> transformNodeToObjects(valueWrapper.asNode(), labelMap))
                                        .toJavaOptional())
                .orElse(Collections.emptyList());
    }

    private List<Object> transformNodeToObjects(Node node, Map<String, GraphLabel> labelMap) {
        List<GraphLabel> labels = node.labels()
                .stream()
                .map(labelMap::get)
                .collect(Collectors.toList());

        return labels.stream()
                .map(
                        label ->
                                Try.of(() -> transformLabelToObject(node, label))
                                        .getOrElseThrow(NebulaException::new))
                .collect(Collectors.toList());
    }

    private Object transformLabelToObject(Node node, GraphLabel label) throws Exception {
        Class<?> objClass = ((GraphVertexType<?>) label).getTypeClass();
        Object obj = objClass.newInstance();
        List<Field> fields = FieldUtils.listFields(objClass);
        for (Field field : fields) {
            setFieldValueFromNode(node, label, obj, field);
        }
        return obj;
    }

    private Object dealFieldReformat(GraphLabel graphLabel, Object value) {
        return Optional.ofNullable(graphLabel)
                .map(label -> label.reformatValue(label.getName(), value))
                .orElse(value);
    }

    private <T> void setFieldValueFromRecord(ResultSet.Record record, GraphLabel graphLabel, T obj, Field field) {
        processField(obj, field, graphLabel, () -> record.get(field.getName()));
    }

    private void setFieldValueFromNode(Node node, GraphLabel label, Object obj, Field field) {
        GraphProperty annotation = field.getAnnotation(GraphProperty.class);
        processField(obj, field, label,
                () ->
                        Try.of(() -> node.properties(label.getName())
                                        .get(annotation.value()))
                                .getOrElseThrow(NebulaException::new));
    }

    private <T> void processField(T obj, Field field, GraphLabel graphLabel, Supplier<ValueWrapper> supplier) {
        GraphProperty annotation = field.getAnnotation(GraphProperty.class);
        if (annotation.propertyTypeEnum().equals(GraphPropertyTypeEnum.GRAPH_VERTEX_ID) &&
                !((GraphVertexType<?>) graphLabel).isIdAsField()) {
            return;
        }

        String key = Optional.of(annotation)
                .map(GraphProperty::value)
                .orElse(field.getName());

        Optional.ofNullable(supplier.get())
                .filter(valueWrapper -> !valueWrapper.isNull())
                .ifPresent(valueWrapper -> setFieldValue(obj, field, graphLabel, key, valueWrapper));
    }

    private <T> void setFieldValue(T obj, Field field, GraphLabel graphLabel, String key, ValueWrapper valueWrapper) {
        Try.run(
                        () -> {
                            field.setAccessible(true);
                            setObjectValueByDataType(obj, field, key, valueWrapper, graphLabel, field.getAnnotation(GraphProperty.class));
                        })
                .getOrElseThrow(NebulaException::new);
    }

    private <T> void setObjectValueByDataType(T obj, Field field, String key, ValueWrapper valueWrapper, GraphLabel graphLabel, GraphProperty annotation) throws IllegalAccessException, UnsupportedEncodingException {
        Object value;
        if (annotation != null &&
                !GraphDataTypeEnum.NULL.equals(annotation.dataType())) {
            value = valueTransformers.get(annotation.dataType()).apply(graphLabel, valueWrapper);
        } else {
            value = determineValueByWrapperType(graphLabel, valueWrapper);
        }
        field.set(obj, value);
    }

    private Object determineValueByWrapperType(GraphLabel graphLabel, ValueWrapper valueWrapper) throws UnsupportedEncodingException {
        if (valueWrapper.isNull()) return dealFieldReformat(graphLabel, valueWrapper.asNull());
        if (valueWrapper.isString()) return dealFieldReformat(graphLabel, valueWrapper.asString());
        if (valueWrapper.isDate()) return dealFieldReformat(graphLabel, valueWrapper.asDate());
        if (valueWrapper.isDateTime()) return dealFieldReformat(graphLabel, valueWrapper.asDateTime());
        if (valueWrapper.isTime()) return dealFieldReformat(graphLabel, valueWrapper.asTime());
        if (valueWrapper.isDouble()) return dealFieldReformat(graphLabel, valueWrapper.asDouble());
        if (valueWrapper.isLong()) return dealFieldReformat(graphLabel, valueWrapper.asLong());
        if (valueWrapper.isBoolean()) return dealFieldReformat(graphLabel, valueWrapper.asBoolean());
        throw new IllegalArgumentException("");
    }

    {
        valueTransformers = new HashMap<>();

        valueTransformers.put(GraphDataTypeEnum.INT,
                (GraphLabel graphLabel, ValueWrapper value) ->
                        Try.of(() -> dealFieldReformat(graphLabel, value.asLong()))
                                .getOrElseThrow(NebulaException::new));

        valueTransformers.put(GraphDataTypeEnum.STRING,
                (GraphLabel graphLabel, ValueWrapper value) ->
                        Try.of(() -> dealFieldReformat(graphLabel, value.asString()))
                                .getOrElseThrow(NebulaException::new));

        valueTransformers.put(GraphDataTypeEnum.DATE,
                (GraphLabel graphLabel, ValueWrapper value) ->
                        Try.of(() -> dealFieldReformat(graphLabel, value.asDate()))
                                .getOrElseThrow(NebulaException::new));

        valueTransformers.put(GraphDataTypeEnum.DATE_TIME,
                (GraphLabel graphLabel, ValueWrapper value) ->
                        Try.of(() -> dealFieldReformat(graphLabel, value.asDateTime()))
                                .getOrElseThrow(NebulaException::new));

        valueTransformers.put(GraphDataTypeEnum.BOOLEAN,
                (GraphLabel graphLabel, ValueWrapper value) ->
                        Try.of(() -> dealFieldReformat(graphLabel, value.asBoolean()))
                                .getOrElseThrow(NebulaException::new));

        valueTransformers.put(GraphDataTypeEnum.TIMESTAMP,
                (GraphLabel graphLabel, ValueWrapper value) ->
                        Try.of(() -> dealFieldReformat(graphLabel, value.asTime()))
                                .getOrElseThrow(NebulaException::new));

        valueTransformers.put(GraphDataTypeEnum.DOUBLE,
                (GraphLabel graphLabel, ValueWrapper value) ->
                        Try.of(() -> dealFieldReformat(graphLabel, value.asDouble()))
                                .getOrElseThrow(NebulaException::new));
    }
}
