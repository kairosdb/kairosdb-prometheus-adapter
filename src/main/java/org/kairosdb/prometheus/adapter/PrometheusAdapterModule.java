package org.kairosdb.prometheus.adapter;

import com.google.inject.AbstractModule;
import com.google.inject.Singleton;

public class PrometheusAdapterModule extends AbstractModule
{
    @Override
    protected void configure()
    {
        bind(ProtocolBufferMessageBodyProvider.class).in(Singleton.class);

        bind(ReadAdapterResource.class).in(Singleton.class);

        bind(WriteAdapterResource.class).in(Singleton.class);
    }
}
