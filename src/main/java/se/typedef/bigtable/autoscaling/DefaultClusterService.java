package se.typedef.bigtable.autoscaling;

import com.google.cloud.bigtable.grpc.BigtableClusterUtilities;
import com.google.cloud.monitoring.v3.MetricServiceClient;
import com.google.monitoring.v3.ListTimeSeriesRequest;
import com.google.monitoring.v3.ProjectName;
import com.google.monitoring.v3.TimeInterval;
import com.google.protobuf.Timestamp;

import java.util.concurrent.TimeUnit;

public class DefaultClusterService implements ClusterService {

  private final BigtableClusterUtilities clusterUtilities;
  private final MetricServiceClient metricServiceClient;

  private DefaultClusterService(
    final BigtableClusterUtilities clusterUtilities,
    final MetricServiceClient metricServiceClient) {
    this.clusterUtilities = clusterUtilities;
    this.metricServiceClient = metricServiceClient;
  }

  public static ClusterService create(
    final BigtableClusterUtilities clusterUtilities,
    final MetricServiceClient metricServiceClient) {
    return new DefaultClusterService(clusterUtilities, metricServiceClient);
  }

  @Override
  public int getClusterSize() {
    return clusterUtilities.getClusterSize();
  }

  @Override
  public void setClusterSize(final int size) throws InterruptedException {
    clusterUtilities.setClusterSize(size);
  }

  @Override
  public double getCpuUsage(final String project, final String instanceId) {
    final long currentTimeSeconds = TimeUnit.MILLISECONDS.toSeconds(System.currentTimeMillis());

    final Timestamp now = Timestamp.newBuilder().setSeconds(currentTimeSeconds).build();
    final Timestamp fiveMinutesAgo =
      Timestamp.newBuilder().setSeconds(currentTimeSeconds - 300).build();
    final TimeInterval interval =
      TimeInterval.newBuilder().setStartTime(fiveMinutesAgo).setEndTime(now).build();

    final String filter =
      "metric.type=\"bigtable.googleapis.com/cluster/cpu_load\""
        + " AND resource.labels.instance=\"" + instanceId + "\"";

    final MetricServiceClient.ListTimeSeriesPagedResponse response =
      metricServiceClient.listTimeSeries(
        ProjectName.of(project), filter, interval, ListTimeSeriesRequest.TimeSeriesView.FULL);

    return response
      .getPage()
      .getValues()
      .iterator()
      .next()
      .getPoints(0)
      .getValue()
      .getDoubleValue();
  }
}
