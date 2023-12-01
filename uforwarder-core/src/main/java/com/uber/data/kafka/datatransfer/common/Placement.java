package com.uber.data.kafka.datatransfer.common;

public interface Placement {

  String DEFAULT_PLACEMENT_REGION = "_region";
  String DEFAULT_PLACEMENT_ZONE = "_zone";

  Placement DEFAULT =
      new Placement() {
        @Override
        public String getRegion() {
          return DEFAULT_PLACEMENT_REGION;
        }

        @Override
        public String getZone() {
          return DEFAULT_PLACEMENT_ZONE;
        }
      };

  /**
   * getRegion gets the deployment region. A deployment region can be a cluster of datacenters, or a
   * set of cloud zones.
   *
   * @return a string containing the region identifier
   */
  String getRegion();

  /**
   * getZone gets the deployment zone. A deployment zone is usually the smallest unit of deployment,
   * it can be a datacenter, or a cloud zone.
   *
   * @return a string containing the zone identifier
   */
  String getZone();
}
