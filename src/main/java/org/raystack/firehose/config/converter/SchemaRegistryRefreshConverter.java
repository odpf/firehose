package org.raystack.firehose.config.converter;

import java.lang.reflect.Method;

import org.aeonbits.owner.Converter;

import org.raystack.stencil.cache.SchemaRefreshStrategy;

public class SchemaRegistryRefreshConverter implements Converter<SchemaRefreshStrategy> {

    @Override
    public SchemaRefreshStrategy convert(Method method, String input) {
        if ("VERSION_BASED_REFRESH".equalsIgnoreCase(input)) {
            return SchemaRefreshStrategy.versionBasedRefresh();
        }
        return SchemaRefreshStrategy.longPollingStrategy();
    }
}
