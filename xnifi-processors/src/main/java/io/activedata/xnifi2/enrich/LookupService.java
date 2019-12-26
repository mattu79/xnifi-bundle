package io.activedata.xnifi2.enrich;

import io.activedata.xnifi2.core.batch.Input;
import org.apache.nifi.controller.ControllerService;
import org.apache.nifi.serialization.record.Record;

import java.util.Map;
import java.util.Optional;

/**
 *
 */
public interface LookupService extends ControllerService {
    Optional<Record> lookup(Input input, Map<String, String> attributes);
}
