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
import org.apache.nifi.annotation.behavior.*;
import org.apache.nifi.annotation.behavior.InputRequirement.Requirement;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.serialization.record.Record;
import org.apache.nifi.util.Tuple;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Optional;


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

    public static final PropertyDescriptor PROP_LOOKUP_SERVICE = new PropertyDescriptor.Builder()
            .name("lookup-service")
            .displayName("检索服务")
            .description("检索服务")
            .identifiesControllerService(LookupService.class)
            .required(true)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();

    private volatile LookupService lookupService;

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        List<PropertyDescriptor> props = new ArrayList<PropertyDescriptor>();
        props.add(PROP_LOOKUP_SERVICE);
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
    }

    @Override
    protected void afterProcess() {
        super.afterProcess();
    }

    @Override
    protected Tuple<Relationship, Output> processRecordInternal(Map<String, String> attributes, Input input, FlowFile original, ProcessContext context) throws Exception {
        Optional<Record> optionalRecord = lookupService.lookup(input, attributes);
        Output output = new Output();
        if (optionalRecord.isPresent()){
            Record record = optionalRecord.get();
            output.putAll(input);
            output.putAll(record.toMap());
            return new Tuple<>(REL_SUCCESS, output);
        }else {
            return new Tuple<>(REL_FAILURE, output);
        }
    }
}
