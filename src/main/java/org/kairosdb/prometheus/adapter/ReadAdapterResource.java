package org.kairosdb.prometheus.adapter;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableSortedMap;
import com.google.common.collect.ImmutableSortedMap.Builder;
import com.google.inject.Inject;
import org.h2.util.StringUtils;
import org.kairosdb.core.DataPoint;
import org.kairosdb.core.datastore.DataPointGroup;
import org.kairosdb.core.datastore.DatastoreQuery;
import org.kairosdb.core.datastore.KairosDatastore;
import org.kairosdb.core.datastore.QueryMetric;
import org.kairosdb.core.exception.DatastoreException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import prometheus.Remote.Query;
import prometheus.Remote.QueryResult;
import prometheus.Remote.ReadRequest;
import prometheus.Remote.ReadResponse;
import prometheus.Types.Label;
import prometheus.Types.LabelMatcher;
import prometheus.Types.Sample;
import prometheus.Types.TimeSeries;

import javax.ws.rs.Consumes;
import javax.ws.rs.POST;
import javax.ws.rs.Path;
import javax.ws.rs.Produces;
import javax.ws.rs.core.MediaType;
import java.util.List;
import java.util.Properties;

import static com.google.common.base.Preconditions.checkState;
import static io.netty.util.internal.ObjectUtil.checkNotNull;

// Note this does not currently work. Decompressing incoming snappy works but when I compress it uses the same library Prometheus can't decompress it.
// I've tried SnappyOutputStream, SnappyHadoopCompatibleOutputStream, Snappy.compress, and SnappyFramedOutputStream without success.
// Prometheus code says it using this framing format https://github.com/google/snappy/blob/master/framing_format.txt which is the same the Java library references.

@Path("api/v1/prometheus/readadapter")
public class ReadAdapterResource
{
    private static final Logger logger = LoggerFactory.getLogger(ReadAdapterResource.class);
    private static final String METRIC_NAME_LABEL = "__name__";

    private final KairosDatastore datastore;
    private final String prefix;

    @Inject
    public ReadAdapterResource(KairosDatastore datastore, Properties config)
    {
        this.datastore = checkNotNull(datastore, "datastore must not be null");
        Preconditions.checkNotNull(config, "config must not be null");

        prefix = config.getProperty("kairosdb.plugin.prometheus-adapter.prefix");
    }

    @SuppressWarnings("ConstantConditions")
    @POST
    @Consumes("application/x-protobuf")
    @Produces(MediaType.WILDCARD)
    @Path("/read")
    public ReadResponse read(ReadRequest request)
    {
        ReadResponse.Builder responseBuilder = ReadResponse.newBuilder();
//        for (Query query : request.getQueriesList()) {
//            String metricName = null;
//            Builder<String, String> tagBuilder = ImmutableSortedMap.naturalOrder();
//            for (LabelMatcher matcher : query.getMatchersList()) {
//                if (matcher.getName().equals("__name__"))
//                {
//                    metricName = prefix != null ? prefix + matcher.getValue() : matcher.getValue();
//                }
//                else
//                {
//                    tagBuilder.put(matcher.getName(), matcher.getValue());
//                }
//            }
//            checkState(!StringUtils.isNullOrEmpty(metricName), "No metric name was specified for the given query. Missing __name__ label.");
//
//            // todo what to do with hints
//            // todo if query returns empty results how to format the response
//
//            // Build Kairos query
//            QueryMetric queryMetric = new QueryMetric(query.getStartTimestampMs(), query.getEndTimestampMs(), 0, metricName);
//            queryMetric.setTags(tagBuilder.build());
//
//            // Execute query and format response
//            logger.info(queryMetric.toString());
//            try (DatastoreQuery datastoreQuery = datastore.createQuery(queryMetric))
//            {
//                List<DataPointGroup> results = datastoreQuery.execute();
//                responseBuilder.addResults(formatPrometheusResponse(results));
//            }
//            catch (DatastoreException e) {
//                e.printStackTrace();
//            }
//        }

        QueryResult.Builder resultBuilder = responseBuilder.addResultsBuilder();
        TimeSeries.Builder timeSeriesBuilder = resultBuilder.addTimeseriesBuilder();
        timeSeriesBuilder.addLabelsBuilder().setName("__name__").setValue("scrape_duration_seconds");
        timeSeriesBuilder.addSamplesBuilder().setTimestamp(1538416287033L).setValue(10);

        ReadResponse response = responseBuilder.build();

        logger.info("response = " + response);
        return response;
    }

    private QueryResult formatPrometheusResponse(List<DataPointGroup> results)
    {
        QueryResult.Builder builder = QueryResult.newBuilder();
        logger.info("Number of results =" + results.size());

        for (DataPointGroup result : results) {
            TimeSeries.Builder timeSeriesBuilder = TimeSeries.newBuilder();

            // todo do this better
            // String off prefix from metric name
            String metricName = result.getName().substring(result.getName().indexOf(prefix) + prefix.length());


            // Add Metric Name
            timeSeriesBuilder.addLabelsBuilder()
                    .setName(METRIC_NAME_LABEL)
                    .setValue(metricName);

            logger.info("Number of tags: " + result.getTagNames().size());
            for (String tagName : result.getTagNames()) {
                // Add tags
                for (String tagValue : result.getTagValues(tagName)) {
                    timeSeriesBuilder.addLabelsBuilder()
                            .setName(tagName)
                            .setValue(tagValue);
                }
            }

            boolean foundDatapoints = false;
            while(result.hasNext())
            {
                // Add data points
                DataPoint dataPoint = result.next();
//                logger.info("datapoint: " + dataPoint.getDoubleValue());
                timeSeriesBuilder.addSamplesBuilder()
                        .setTimestamp(dataPoint.getTimestamp())
                        .setValue(dataPoint.getDoubleValue());
                foundDatapoints = true;
            }

            if (foundDatapoints)
            {
                builder.addTimeseries(timeSeriesBuilder);
            }
        }
        return builder.build();
    }

}
