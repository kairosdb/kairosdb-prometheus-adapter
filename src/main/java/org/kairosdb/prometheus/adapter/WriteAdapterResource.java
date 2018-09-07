package org.kairosdb.prometheus.adapter;

import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedMap.Builder;
import com.google.inject.Inject;
import org.h2.util.StringUtils;
import org.kairosdb.core.datapoints.DoubleDataPoint;
import org.kairosdb.core.datapoints.LongDataPoint;
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

import java.net.InetAddress;
import java.net.UnknownHostException;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Path("api/v1/prometheus/writeadapter")
public class WriteAdapterResource
{
    private static final Logger logger = LoggerFactory.getLogger(WriteAdapterResource.class);
    public static final String METRIC_METRICS_SENT = "prometheus.write-adapter.metrics-sent.count";
    public static final String METRIC_EXCEPTIONS = "prometheus.write-adapter.exception.count";

    private final Publisher<DataPointEvent> dataPointPublisher;
    private final String host;

    // todo Prefix from configuration?
    // todo replace instance tag with "host"

    @Inject
    public WriteAdapterResource(FilterEventBus eventBus)
            throws UnknownHostException
    {
        checkNotNull(eventBus, "eventBus must not be null");
        this.dataPointPublisher = eventBus.createPublisher(DataPointEvent.class);
        host = InetAddress.getLocalHost().getHostName();
    }

    @SuppressWarnings("ConstantConditions")
    //    @Consumes("application/protobuf")
    @POST
    @Consumes(MediaType.WILDCARD) // Todo Is there a better way?
    @Produces("text/plain")
    @Path("/write")
    public Response write(WriteRequest request)
    {
        int metricsSent = 0;
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

                checkState(!StringUtils.isNullOrEmpty(metricName), "No metric name was specified for the given metric. Missing __name__ label." );

                for (Sample sample : timeSeries.getSamplesList()) {
                    publishMetric(metricName, sample.getTimestamp(), sample.getValue(), tagBuilder.build());
                    metricsSent++;
                }

                publishMetric(METRIC_METRICS_SENT, metricsSent, "success", "true");
                publishMetric(METRIC_METRICS_SENT, request.getTimeseriesList().size() - metricsSent, "success", "false");
            }

            return Response.status(Response.Status.OK).build();
        }
        catch (Exception e) {
            logger.error("Error processing request: " + request.toString(), e);
            publishMetric(METRIC_EXCEPTIONS, 1, "exception", e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

    private void publishMetric(String metricName, long value, String tagName, String tagValue)
    {
        dataPointPublisher.post(new DataPointEvent(metricName,
                ImmutableSortedMap.of("host", host, tagName, tagValue),
                new LongDataPoint(System.currentTimeMillis(), value)));
    }

    private void publishMetric(String metricName, long timestamp, double value, ImmutableSortedMap<String, String> tags)
    {
        dataPointPublisher.post(new DataPointEvent(metricName,
                tags,
                new DoubleDataPoint(timestamp, value)));
    }
}
