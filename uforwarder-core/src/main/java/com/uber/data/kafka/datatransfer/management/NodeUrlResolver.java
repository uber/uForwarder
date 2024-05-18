package com.uber.data.kafka.datatransfer.management;

import com.uber.data.kafka.datatransfer.Node;

/** Resolves management web UI link for node */
public class NodeUrlResolver {

  private static final String WORKER_URL_FORMAT = "http://%s/jobs";

  /**
   * Resolves management web UI link for node
   *
   * @param node
   * @return link to the management UI of the node
   */
  public String resolveLink(Node node) {
    String hostPort = String.format("%s:%d", node.getHost(), node.getPort());
    return String.format(WORKER_URL_FORMAT, hostPort);
  }
}
