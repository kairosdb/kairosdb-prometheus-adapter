package org.kairosdb.prometheus.adapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prometheus.Remote.ReadRequest;
import prometheus.Remote.ReadResponse;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;

@Path("api/v1/prometheus/readadapter")
public class ReadAdapterResource
{
    private static final Logger logger = LoggerFactory.getLogger(ReadAdapterResource.class);

    @POST
    @Consumes("application/protobuf")
    @Path("/read")
    public ReadResponse read(ReadRequest request)
    {
        logger.info("in read");
        return null;
    }

}
