package se.typedef.bigtable.autoscaling;

import org.junit.Test;

import java.time.Clock;
import java.time.Duration;
import java.time.Instant;
import java.time.ZoneId;

import static org.junit.Assert.assertEquals;

public class BigtableScalerTest {

  private static final String PROJECT = "project";
  private static final String INSTANCE_ID = "instance";
  private static final int INCREASE_RATE = 5;
  private static final int DECREASE_RATE = 3;
  private static final Duration COOLDOWN = Duration.ofSeconds(10);

  @Test
  public void increase_size_when_cpu_usage_too_high_and_not_reach_max_cluster_size() {
    final int clusterSize = 10;
    final double cpuUsage = 0.8;
    final FakeClusterService clusterService = new FakeClusterService(clusterSize, cpuUsage);

    final Clock clock = Clock.fixed(Instant.ofEpochMilli(1519563099032L), ZoneId.of("UTC"));

    final BigtableScaler scaler =
      BigtableScaler.scaler(PROJECT, INSTANCE_ID, clock, clusterService)
        .maxCpuUsage(0.5)
        .minCpuUsage(0.2)
        .maxNodeCount(20)
        .minNodeCount(5)
        .increaseRate(INCREASE_RATE)
        .decreaseRate(DECREASE_RATE)
        .cooldown(COOLDOWN)
        .build();

    scaler.run();

    assertEquals(clusterSize + INCREASE_RATE, clusterService.getClusterSize());
  }

  @Test
  public void decrease_size_when_cpu_usage_too_low_and_not_reach_min_cluster_size() {
    final int clusterSize = 10;
    final double cpuUsage = 0.1;
    final FakeClusterService clusterService = new FakeClusterService(clusterSize, cpuUsage);

    final Clock clock = Clock.fixed(Instant.ofEpochMilli(1519563099032L), ZoneId.of("UTC"));

    final BigtableScaler scaler =
      BigtableScaler.scaler(PROJECT, INSTANCE_ID, clock, clusterService)
        .maxCpuUsage(0.5)
        .minCpuUsage(0.2)
        .maxNodeCount(20)
        .minNodeCount(5)
        .increaseRate(INCREASE_RATE)
        .decreaseRate(DECREASE_RATE)
        .cooldown(COOLDOWN)
        .build();

    scaler.run();

    assertEquals(clusterSize - DECREASE_RATE, clusterService.getClusterSize());
  }

  @Test
  public void do_not_increase_size_when_cpu_usage_too_high_but_reached_max_cluster_size() {
    final int clusterSize = 10;
    final double cpuUsage = 0.8;
    final FakeClusterService clusterService = new FakeClusterService(clusterSize, cpuUsage);

    final Clock clock = Clock.fixed(Instant.ofEpochMilli(1519563099032L), ZoneId.of("UTC"));

    final BigtableScaler scaler =
      BigtableScaler.scaler(PROJECT, INSTANCE_ID, clock, clusterService)
        .maxCpuUsage(0.5)
        .minCpuUsage(0.2)
        .maxNodeCount(10)
        .minNodeCount(5)
        .increaseRate(INCREASE_RATE)
        .decreaseRate(DECREASE_RATE)
        .cooldown(COOLDOWN)
        .build();

    scaler.run();

    assertEquals(clusterSize, clusterService.getClusterSize());
  }

  @Test
  public void do_not_decrease_size_when_cpu_usage_too_low_but_reached_min_cluster_size() {
    final int clusterSize = 10;
    final double cpuUsage = 0.1;
    final FakeClusterService clusterService = new FakeClusterService(clusterSize, cpuUsage);

    final Clock clock = Clock.fixed(Instant.ofEpochMilli(1519563099032L), ZoneId.of("UTC"));

    final BigtableScaler scaler =
      BigtableScaler.scaler(PROJECT, INSTANCE_ID, clock, clusterService)
        .maxCpuUsage(0.5)
        .minCpuUsage(0.2)
        .maxNodeCount(20)
        .minNodeCount(10)
        .increaseRate(INCREASE_RATE)
        .decreaseRate(DECREASE_RATE)
        .cooldown(COOLDOWN)
        .build();

    scaler.run();

    assertEquals(clusterSize, clusterService.getClusterSize());
  }

  @Test
  public void do_nothing_if_load_is_ok() {
    final int clusterSize = 10;
    final double cpuUsage = 0.4;
    final FakeClusterService clusterService = new FakeClusterService(clusterSize, cpuUsage);

    final Clock clock = Clock.fixed(Instant.ofEpochMilli(1519563099032L), ZoneId.of("UTC"));

    final BigtableScaler scaler =
      BigtableScaler.scaler(PROJECT, INSTANCE_ID, clock, clusterService)
        .maxCpuUsage(0.9)
        .minCpuUsage(0.1)
        .maxNodeCount(20)
        .minNodeCount(5)
        .increaseRate(INCREASE_RATE)
        .decreaseRate(DECREASE_RATE)
        .cooldown(COOLDOWN)
        .build();

    scaler.run();

    assertEquals(clusterSize, clusterService.getClusterSize());
  }

  @Test
  public void respect_cooldown_on_too_high_load() {
    final int clusterSize = 10;
    final double cpuUsage = 0.9;
    final FakeClusterService clusterService = new FakeClusterService(clusterSize, cpuUsage);

    final Clock clock = Clock.fixed(Instant.ofEpochMilli(1519563099032L), ZoneId.of("UTC"));

    final BigtableScaler scaler =
      BigtableScaler.scaler(PROJECT, INSTANCE_ID, clock, clusterService)
        .maxCpuUsage(0.4)
        .minCpuUsage(0.1)
        .maxNodeCount(20)
        .minNodeCount(5)
        .increaseRate(INCREASE_RATE)
        .decreaseRate(DECREASE_RATE)
        .cooldown(COOLDOWN)
        .build();

    scaler.run();
    scaler.run();
    scaler.run();
    scaler.run();

    assertEquals(clusterSize + INCREASE_RATE, clusterService.getClusterSize());
  }

  @Test
  public void respect_cooldown_on_too_low_load() {
    final int clusterSize = 10;
    final double cpuUsage = 0.1;
    final FakeClusterService clusterService = new FakeClusterService(clusterSize, cpuUsage);

    final Clock clock = Clock.fixed(Instant.ofEpochMilli(1519563099032L), ZoneId.of("UTC"));

    final BigtableScaler scaler =
      BigtableScaler.scaler(PROJECT, INSTANCE_ID, clock, clusterService)
        .maxCpuUsage(0.9)
        .minCpuUsage(0.5)
        .maxNodeCount(20)
        .minNodeCount(5)
        .increaseRate(INCREASE_RATE)
        .decreaseRate(DECREASE_RATE)
        .cooldown(COOLDOWN)
        .build();

    scaler.run();
    scaler.run();
    scaler.run();
    scaler.run();

    assertEquals(clusterSize - DECREASE_RATE, clusterService.getClusterSize());
  }

  private static class FakeClusterService implements ClusterService {

    private int size;
    private double cpuUsage;

    public FakeClusterService(final int initialSize, final double initialCpuUsage) {
      this.size = initialSize;
      this.cpuUsage = initialCpuUsage;
    }

    @Override
    public synchronized int getClusterSize() {
      return size;
    }

    @Override
    public synchronized void setClusterSize(final int size) {
      this.size = size;
    }

    @Override
    public synchronized double getCpuUsage(final String project, final String instanceId) {
      return cpuUsage;
    }
  }
}
