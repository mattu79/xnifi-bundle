/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.activedata.xnifi2.processors.enrich;

import io.activedata.xnifi2.core.batch.AbstractBuilderSupportProcessor;
import io.activedata.xnifi2.core.batch.Input;
import io.activedata.xnifi2.core.batch.Output;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.AllowableValue;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.Tuple;

import java.util.*;


@EventDriven
@SideEffectFree
@SupportsBatching
@InputRequirement(Requirement.INPUT_REQUIRED)
@WritesAttributes({
        @WritesAttribute(attribute = "mime.type", description = "数据格式"),
        @WritesAttribute(attribute = "record.count", description = "FlowFile中记录的数量")
})
@Tags({"lookup", "enrichment", "route", "record", "csv", "json", "avro", "database", "db", "logs", "convert", "filter"})
@CapabilityDescription("通过检索服务循环查询出对应的结果记录。当检索结果返回时，结果可以可选的附加到原始记录上。")
public class EnrichRecord extends AbstractBuilderSupportProcessor {

    static final AllowableValue ROUTE_TO_SUCCESS = new AllowableValue("route-to-success", "全部路由到成功队列",
            "不管是否检索到记录都路由到成功队列");

    static final AllowableValue ROUTE_TO_MATCHED_UNMATCHED = new AllowableValue("route-to-matched-unmatched", "路由到匹配或未匹配队列",
            "检索到记录时路由到匹配队列，未检索到记录时路由到非匹配队列");

    public static final PropertyDescriptor PROP_LOOKUP_SERVICE = new PropertyDescriptor.Builder()
            .name("lookup-service")
            .displayName("检索服务")
            .description("检索服务")
            .identifiesControllerService(LookupService.class)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    public static final PropertyDescriptor PROP_ROUTING_STRATEGY = new PropertyDescriptor.Builder()
            .name("routing-strategy")
            .displayName("路由策略")
            .description("指定未检索到记录时的路由策略")
            .expressionLanguageSupported(ExpressionLanguageScope.NONE)
            .allowableValues(ROUTE_TO_SUCCESS, ROUTE_TO_MATCHED_UNMATCHED)
            .defaultValue(ROUTE_TO_SUCCESS.getValue())
            .required(true)
            .build();

    public static final Relationship REL_MATCHED = new Relationship.Builder()
            .name("matched")
            .description("匹配队列")
            .build();

    public static final Relationship REL_UNMATCHED = new Relationship.Builder()
            .name("unmatched")
            .description("为匹配队列")
            .build();

    private Set<Relationship> rels;

    private volatile boolean routeToMatchedUnmatched;

    private volatile LookupService lookupService;

    private volatile String routingStrategy;

    @Override
    public void onPropertyModified(final PropertyDescriptor descriptor, final String oldValue, final String newValue) {
        if (PROP_ROUTING_STRATEGY.equals(descriptor)) {
            if (ROUTE_TO_MATCHED_UNMATCHED.getValue().equalsIgnoreCase(newValue)) {
                final Set<Relationship> matchedUnmatchedRels = new HashSet<>();
                matchedUnmatchedRels.add(REL_MATCHED);
                matchedUnmatchedRels.add(REL_UNMATCHED);
                matchedUnmatchedRels.add(REL_FAILURE);
                this.rels = matchedUnmatchedRels;
                this.routeToMatchedUnmatched = true;
            } else {
                final Set<Relationship> successRels = new HashSet<>();
                successRels.add(REL_SUCCESS);
                successRels.add(REL_FAILURE);
                this.rels = successRels;
                this.routeToMatchedUnmatched = false;
            }
        }
    }

    @Override
    public Set<Relationship> getRelationships() {
        if (this.rels == null) {
            final Set<Relationship> successRels = new HashSet<>();
            successRels.add(REL_SUCCESS);
            successRels.add(REL_FAILURE);
            this.rels = successRels;
            this.routeToMatchedUnmatched = false;
        }
        return this.rels;
    }

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<PropertyDescriptor>();
        props.add(PROP_LOOKUP_SERVICE);
        props.add(PROP_ROUTING_STRATEGY);
        props.add(PROP_OUTPUT_RECORD_STRATEGY);
        props.add(PROP_INPUT_RECORD_TYPE);
        props.add(PROP_OUTPUT_RECORD_TYPE);
        props.add(PROP_OUTPUT_RECORD_EXAMPLE);
        props.add(PROP_RECORD_INPUT_BUILDER);
        props.add(PROP_RECORD_OUTPUT_BUILDER);
        return props;
    }

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        super.beforeProcess(context);

        lookupService = context.getProperty(PROP_LOOKUP_SERVICE).asControllerService(LookupService.class);
        routingStrategy = context.getProperty(PROP_ROUTING_STRATEGY).getValue();
        routeToMatchedUnmatched = StringUtils.equals(routingStrategy, ROUTE_TO_MATCHED_UNMATCHED.getValue());
    }

    @Override
    protected void afterProcess() {
        super.afterProcess();
    }

    @Override
    protected Tuple<Relationship, Output> processRecordInternal(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws Exception {
        Optional<Record> optionalRecord = lookupService.lookup(input, attributes);
        Output output = new Output();
        if (optionalRecord.isPresent()) {
            Record record = optionalRecord.get();
            output.putAll(input);
            output.putAll(record.toMap());

            if (routeToMatchedUnmatched) {
                return new Tuple<>(REL_MATCHED, output);
            } else {
                return new Tuple<>(REL_SUCCESS, output);
            }
        } else {
            if (routeToMatchedUnmatched) {
                return new Tuple<>(REL_UNMATCHED, output);
            } else {
                return new Tuple<>(REL_SUCCESS, output);
            }
        }
    }
}
