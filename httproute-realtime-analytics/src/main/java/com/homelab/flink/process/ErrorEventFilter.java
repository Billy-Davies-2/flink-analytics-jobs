package com.homelab.flink.process;

import com.homelab.flink.model.AccessLog;

import org.apache.flink.api.common.functions.FilterFunction;

/**
 * Filter function that passes through only error events (4xx and 5xx responses).
 */
public class ErrorEventFilter implements FilterFunction<AccessLog> {

    private static final long serialVersionUID = 1L;

    @Override
    public boolean filter(AccessLog log) throws Exception {
        if (log == null) {
            return false;
        }
        return log.isError();
    }
}
