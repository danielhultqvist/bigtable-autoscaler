package se.typedef.bigtable.autoscaling;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;

public class BigtableScaler implements Runnable {

  private static final Logger LOG = LoggerFactory.getLogger(BigtableScaler.class);

  private final String project;
  private final String instanceId;
  private final Clock clock;
  private final ClusterService clusterService;
  private final int minClusterSize;
  private final int maxClusterSize;
  private final int increaseNodeRate;
  private final int decreaseNodeRate;
  private final double maxCpuUsage;
  private final double minCpuUsage;
  private final Duration cooldown;

  private Instant lastAdjustmentTime = Instant.EPOCH;

  private BigtableScaler(
    final String project,
    final String instanceId,
    final Clock clock,
    final ClusterService clusterService,
    final int minClusterSize,
    final int maxClusterSize,
    final int increaseNodeRate,
    final int decreaseNodeRate,
    final double maxCpuUsage,
    final double minCpuUsage,
    final Duration cooldown) {
    this.project = project;
    this.instanceId = instanceId;
    this.clock = clock;
    this.clusterService = clusterService;
    this.minClusterSize = minClusterSize;
    this.maxClusterSize = maxClusterSize;
    this.increaseNodeRate = increaseNodeRate;
    this.decreaseNodeRate = decreaseNodeRate;
    this.maxCpuUsage = maxCpuUsage;
    this.minCpuUsage = minCpuUsage;
    this.cooldown = cooldown;
  }

  public static BigtableScaler.Builder scaler(
    final String project,
    final String instanceId,
    final Clock clock,
    final ClusterService clusterService) {
    return new BigtableScaler.Builder(project, instanceId, clock, clusterService);
  }

  @Override
  public void run() {
    final Instant currentTime = clock.instant();
    if (lastAdjustmentTime.isBefore(currentTime.minus(cooldown))) {
      try {
        final double cpuUsage = clusterService.getCpuUsage(project, instanceId);
        final int clusterSize = clusterService.getClusterSize();

        if (cpuUsage > maxCpuUsage) {
          if (clusterSize < maxClusterSize) {
            increaseClusterSize(clusterService, clusterSize, cpuUsage);
          } else {
            LOG.info("Wanted to increase cluster size, but max count is reached");
          }
        } else if (cpuUsage < minCpuUsage) {
          if (clusterSize > minClusterSize) {
            decreaseClusterSize(clusterService, clusterSize, cpuUsage);
          } else {
            LOG.info("Wanted to decrease cluster size, but min count is reached");
          }
        } else {
          LOG.info("Cluster is running at good scale");
        }
      } catch (Exception e) {
        LOG.error("Failure to auto-scale Bigtable", e);
        throw new IllegalStateException(e);
      }
    } else {
      LOG.info("Cooling down, cooldown will complete at {}",
        lastAdjustmentTime.plus(cooldown).toString());
    }
  }

  private void increaseClusterSize(
    final ClusterService clusterService,
    final int clusterSize,
    final double cpuUsage) throws InterruptedException {
    final int newClusterSize = Math.min(clusterSize + increaseNodeRate, maxClusterSize);
    LOG.info("Increasing cluster size from {} to {}, usage at {}", clusterSize, newClusterSize, cpuUsage);
    clusterService.setClusterSize(newClusterSize);
    lastAdjustmentTime = clock.instant();
  }

  private void decreaseClusterSize(
    final ClusterService clusterService,
    final int clusterSize,
    final double cpuUsage) throws InterruptedException {
    final int newClusterSize = Math.max(clusterSize - decreaseNodeRate, minClusterSize);
    LOG.info("Decreasing cluster size from {} to {}, usage at {}", clusterSize, newClusterSize, cpuUsage);
    clusterService.setClusterSize(newClusterSize);
    lastAdjustmentTime = clock.instant();
  }

  public static class Builder {

    private final String project;
    private final String instanceId;
    private final Clock clock;
    private final ClusterService clusterService;

    private int minNodeCount = 3;
    private int maxNodeCount = 30;
    private int increaseRate = 3;
    private int decreaseRate = 3;

    private double maxCpuUsage = 0.7;
    private double minCpuUsage = 0.15;

    private Duration cooldown = Duration.ofMinutes(20);

    private Builder(
      final String project,
      final String instanceId,
      final Clock clock,
      final ClusterService clusterService) {
      this.project = project;
      this.instanceId = instanceId;
      this.clock = clock;
      this.clusterService = clusterService;
    }

    public Builder minNodeCount(final int minNodeCount) {
      this.minNodeCount = minNodeCount;
      return this;
    }

    public Builder maxNodeCount(final int maxNodeCount) {
      this.maxNodeCount = maxNodeCount;
      return this;
    }

    public Builder increaseRate(final int increaseRate) {
      this.increaseRate = Math.abs(increaseRate);
      return this;
    }

    public Builder decreaseRate(final int decreaseRate) {
      this.decreaseRate = Math.abs(decreaseRate);
      return this;
    }

    public Builder maxCpuUsage(final double maxCpuUsage) {
      this.maxCpuUsage = maxCpuUsage;
      return this;
    }

    public Builder minCpuUsage(final double minCpuUsage) {
      this.minCpuUsage = minCpuUsage;
      return this;
    }

    public Builder cooldown(final Duration cooldown) {
      this.cooldown = cooldown;
      return this;
    }

    public BigtableScaler build() {
      return new BigtableScaler(
        project,
        instanceId,
        clock,
        clusterService,
        minNodeCount,
        maxNodeCount,
        increaseRate,
        decreaseRate,
        maxCpuUsage,
        minCpuUsage,
        cooldown);
    }
  }
}
