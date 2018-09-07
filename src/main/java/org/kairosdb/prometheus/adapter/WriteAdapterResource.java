package org.kairosdb.prometheus.adapter;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedMap.Builder;
import com.google.inject.Inject;
import org.apache.http.entity.StringEntity;
import org.h2.util.StringUtils;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.eventbus.FilterEventBus;
import org.kairosdb.eventbus.Publisher;
import org.kairosdb.events.DataPointEvent;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prometheus.Remote.WriteRequest;
import prometheus.Types.Label;
import prometheus.Types.Sample;
import prometheus.Types.TimeSeries;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import javax.ws.rs.core.Response;
import java.nio.charset.Charset;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Path("api/v1/prometheus/writeadapter")
public class WriteAdapterResource
{
    private static final Logger logger = LoggerFactory.getLogger(WriteAdapterResource.class);

    private final Publisher<DataPointEvent> dataPointPublisher;

    @Inject
    public WriteAdapterResource(FilterEventBus eventBus)
    {
        checkNotNull(eventBus, "eventBus must not be null");
        this.dataPointPublisher = eventBus.createPublisher(DataPointEvent.class);
    }

    @SuppressWarnings("ConstantConditions")
    @POST
    @Consumes(MediaType.WILDCARD) // Todo Is there a better way?
//    @Consumes("application/protobuf")
    @Produces("text/plain")
    @Path("/write")
    public Response write(WriteRequest request)
    {
        try {
            for (TimeSeries timeSeries : request.getTimeseriesList()) {
                String metricName = null;
                Builder<String, String> tagBuilder = ImmutableSortedMap.naturalOrder();
                for (Label label : timeSeries.getLabelsList()) {
                    if (label.getName().equals("__name__")) {
                        metricName = label.getValue();
                    }
                    else {
                        tagBuilder.put(label.getName(), label.getValue());
                    }
                }

                checkState(StringUtils.isNullOrEmpty(metricName), "No metric name was specified for the given metric.");

                for (Sample sample : timeSeries.getSamplesList()) {
                    DoubleDataPoint dataPoint = new DoubleDataPoint(sample.getTimestamp(), sample.getValue());
                    dataPointPublisher.post(new DataPointEvent(metricName, tagBuilder.build(), dataPoint));
                }
            }

            return Response.status(Response.Status.OK).build();
        }
        catch (Exception e) {
            logger.error("Error processing request: " + request.toString(), e);
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(
                    new StringEntity(e.getMessage(), Charset.forName("UTF-8"))).build();
        }
    }
}
