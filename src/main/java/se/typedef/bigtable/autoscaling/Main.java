package se.typedef.bigtable.autoscaling;

import com.google.cloud.bigtable.grpc.BigtableClusterUtilities;
import com.google.cloud.monitoring.v3.MetricServiceClient;

import java.time.Clock;
import java.time.Duration;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class Main {

  private static final Duration CHECK_RATE = Duration.ofSeconds(10);
  private static final String PROJECT = "project";
  private static final String INSTANCE_ID = "instance";

  public static void main(String[] args) throws Exception {
    final MetricServiceClient metricServiceClient = MetricServiceClient.create();
    final BigtableClusterUtilities bigtableClusterUtilities =
      BigtableClusterUtilities.forInstance(PROJECT, INSTANCE_ID);

    final ClusterService clusterService =
      DefaultClusterService.create(bigtableClusterUtilities, metricServiceClient);

    final BigtableScaler bigtableScaler =
      BigtableScaler.scaler(PROJECT, INSTANCE_ID, Clock.systemUTC(), clusterService)
        .maxCpuUsage(0.7)
        .minCpuUsage(0.10)
        .maxNodeCount(60)
        .minNodeCount(10)
        .increaseRate(5)
        .decreaseRate(3)
        .cooldown(Duration.ofSeconds(30))
        .build();

    final ScheduledExecutorService autoscalingExecutor = Executors.newScheduledThreadPool(1);
    autoscalingExecutor.scheduleWithFixedDelay(
      bigtableScaler,
      0,
      CHECK_RATE.toMillis(),
      TimeUnit.MILLISECONDS
    );

    Runtime.getRuntime().addShutdownHook(new Thread(() -> {
      bigtableClusterUtilities.close();
      try {
        metricServiceClient.close();
      } catch (Exception e) {
        throw new IllegalStateException(e);
      }
    }));
  }
}
