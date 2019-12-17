package io.activedata.xnifi.processors.route;

import io.activedata.xnifi.utils.FlowFileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.nifi.annotation.behavior.InputRequirement;
import org.apache.nifi.annotation.behavior.SupportsBatching;
import org.apache.nifi.annotation.behavior.TriggerSerially;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.mvel2.MVEL;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Created by MattU on 2017/12/27.
 */
@TriggerSerially
@SupportsBatching
@Tags({ "route", "json"})
@InputRequirement(InputRequirement.Requirement.INPUT_REQUIRED)
@CapabilityDescription("对JSON记录进行过滤路由")
public class RouteOnJson extends AbstractRouteJsonProcessor {

    private volatile Map<Relationship, Serializable> compileRouteExprMap;

    @Override
    protected Set<Relationship> route(Map inputContext, FlowFile flowFile, ProcessContext context, Object flowFileContext) {
        Set<Relationship> rels = new HashSet<>();
        for (Map.Entry<Relationship, Serializable> entry : compileRouteExprMap.entrySet()){
            Boolean matched = MVEL.executeExpression(entry.getValue(), inputContext, Boolean.class);
            if (Boolean.TRUE.equals(matched)){
                rels.add(entry.getKey());
            }
        }

        if (rels.size() == 0)
            rels.add(RouteOnJson.REL_UNMATCHED);

        return rels;
    }

    @Override
    protected boolean isRouteOriginal() {
        return false;
    }

    @Override
    protected Object getFlowFileContext(FlowFile flowFile, ProcessContext context) {
        return null;
    }

    @Override
    protected void beforeProcess(ProcessContext context) throws ProcessException {
        compileRouteExprMap = new HashMap<>();
        Map<String, String> routeExprMap = FlowFileUtils.getDynamicProperties(context);
        Set<Relationship> rels = routeRels;
        for (Relationship rel : rels){
            String routeExprText = routeExprMap.get(rel.getName());
            if (StringUtils.isNotBlank(routeExprText)){
                Serializable compileRouteExpr = MVEL.compileExpression(routeExprText);
                compileRouteExprMap.put(rel, compileRouteExpr);
            }
        }

        super.beforeProcess(context);
    }
}
