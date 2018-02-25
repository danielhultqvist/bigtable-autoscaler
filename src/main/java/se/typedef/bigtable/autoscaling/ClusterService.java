package se.typedef.bigtable.autoscaling;

public interface ClusterService {
  int getClusterSize();

  void setClusterSize(int size) throws InterruptedException;

  double getCpuUsage(String project, String instanceId);
}
