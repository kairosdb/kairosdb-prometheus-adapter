package org.kairosdb.prometheus.adapter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prometheus.Remote.WriteRequest;
import prometheus.Types.Label;
import prometheus.Types.Sample;
import prometheus.Types.TimeSeries;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.core.MediaType;

@Path("api/v1/prometheus/writeadapter")
public class WriteAdapterResource
{
    private static final Logger logger = LoggerFactory.getLogger(WriteAdapterResource.class);

    @POST
    @Consumes(MediaType.WILDCARD)
//    @Consumes("application/protobuf")
    @Path("/write")
    public void write(WriteRequest request)
    {
        for (TimeSeries timeSeries : request.getTimeseriesList()) {
            for (Label label : timeSeries.getLabelsList()) {
                if (label.getName().equals("__name__"))
                {
                    logger.info("metric name: " + label.getValue());
                }
                else {
                    logger.info(label.getName() + ":" + label.getValue());
                }
            }

            for (Sample sample : timeSeries.getSamplesList()) {
                logger.info("timestamp: " + sample.getTimestamp());
                logger.info("value:" + sample.getValue());
            }
        }
    }
}
