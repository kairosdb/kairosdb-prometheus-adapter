package org.kairosdb.prometheus.adapter;

import com.google.common.io.ByteStreams;
import org.xerial.snappy.Snappy;

import javax.ws.rs.WebApplicationException;
import javax.ws.rs.ext.Provider;
import javax.ws.rs.ext.ReaderInterceptor;
import javax.ws.rs.ext.ReaderInterceptorContext;
import java.io.ByteArrayInputStream;
import java.io.IOException;

@Provider
public class SnappyReaderInterceptor implements ReaderInterceptor
{
    @Override
    public Object aroundReadFrom(ReaderInterceptorContext context)
            throws IOException, WebApplicationException
    {
        context.setInputStream(new ByteArrayInputStream(Snappy.uncompress(ByteStreams.toByteArray(context.getInputStream()))));
        return context.proceed();
    }
}
