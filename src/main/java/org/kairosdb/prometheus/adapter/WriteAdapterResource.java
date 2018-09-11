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
import java.util.HashSet;
import java.util.Properties;
import java.util.Set;
import java.util.regex.Pattern;

import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;

@Path("api/v1/prometheus/writeadapter")
public class WriteAdapterResource
{
    private static final Logger logger = LoggerFactory.getLogger(WriteAdapterResource.class);
    private static final String METRIC_METRICS_SENT = "kairosdb.prometheus.write-adapter.metrics-sent.count";
    private static final String METRIC_EXCEPTIONS = "kairosdb.prometheus.write-adapter.exception.count";

    private final Publisher<DataPointEvent> dataPointPublisher;
    private final String host;
    private final String prefix;
    private final Set<Pattern> dropMetricsRegex = new HashSet<>();
    private final Set<Pattern> dropLablelsRegex = new HashSet<>();

    @Inject
    public WriteAdapterResource(FilterEventBus eventBus, Properties config)
            throws UnknownHostException
    {
        checkNotNull(eventBus, "eventBus must not be null");
        checkNotNull(config, "config must not be null");
        this.dataPointPublisher = eventBus.createPublisher(DataPointEvent.class);
        host = InetAddress.getLocalHost().getHostName();

        prefix = config.getProperty("kairosdb.plugin.prometheus-adapter.writer.prefix");
        String dropMetrics = config.getProperty("kairosdb.plugin.prometheus-adapter.writer.dropMetrics");
        String dropLabels = config.getProperty("kairosdb.plugin.prometheus-adapter.writer.dropLabels");
        createRegexPatterns(dropMetrics, dropMetricsRegex);
        createRegexPatterns(dropLabels, dropLablelsRegex);
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
        int metricsDropped = 0;
        try {
            for (TimeSeries timeSeries : request.getTimeseriesList()) {
                String metricName = null;
                Builder<String, String> tagBuilder = ImmutableSortedMap.naturalOrder();
                for (Label label : timeSeries.getLabelsList()) {
                    if (label.getName().equals("__name__")) {
                        metricName = prefix != null ? prefix + label.getValue() : label.getValue();
                    }
                    else {
                        if (shouldKeep(label.getName(), dropLablelsRegex))
                        {
                            tagBuilder.put(label.getName(), label.getValue());
                        }
                    }
                }

                checkState(!StringUtils.isNullOrEmpty(metricName), "No metric name was specified for the given metric. Missing __name__ label.");

                if (shouldKeep(metricName, dropMetricsRegex)) {
                    for (Sample sample : timeSeries.getSamplesList()) {
                        publishMetric(metricName, sample.getTimestamp(), sample.getValue(), tagBuilder.build());
                        metricsSent++;
                    }
                }
                else
                {
                    metricsDropped++;
                }
            }

            publishMetrics(request.getTimeseriesList().size(), metricsSent, metricsDropped);

            return Response.status(Response.Status.OK).build();
        }
        catch (Throwable e) {
            logger.error("Error processing request: " + request.toString(), e);
            publishMetric(METRIC_EXCEPTIONS, 1, "exception", e.getMessage());
            return Response.status(Response.Status.INTERNAL_SERVER_ERROR).entity(e.getMessage()).build();
        }
    }

    private void publishMetrics(int metricsReceived, int metricsSent, int metricsDropped)
    {
        publishMetric(METRIC_METRICS_SENT, metricsSent, "status", "sent");

        if (metricsReceived - metricsSent > 0) {
            publishMetric(METRIC_METRICS_SENT, metricsReceived - metricsSent - metricsDropped, "status", "failed");
        }

        if (metricsDropped > 0) {
            publishMetric(METRIC_METRICS_SENT, metricsDropped, "status", "dropped");
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

    private static void createRegexPatterns(String patterns, Set<Pattern> patternSet)
    {
        if (!StringUtils.isNullOrEmpty(patterns)) {
            String[] split = patterns.split("\\s*,\\s*");
            for (String pattern : split) {
                patternSet.add(Pattern.compile(pattern));
            }
        }
    }

    private static boolean shouldKeep(String value, Set<Pattern> patternSet)
    {
        for (Pattern pattern : patternSet) {
            if (pattern.matcher(value).matches()) {
                return false;
            }
        }
        return true;
    }
}
