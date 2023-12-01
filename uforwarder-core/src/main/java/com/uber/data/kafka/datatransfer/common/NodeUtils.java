package com.uber.data.kafka.datatransfer.common;

import com.google.common.net.HostAndPort;
import com.uber.data.kafka.datatransfer.Node;

/**
 * NodeUtils for simplifying creating and updating Node proto entities.
 *
 * <p>The following naming conventions shall be used:
 *
 * <ol>
 *   <li>X getX() returns a field within an object.
 *   <li>X newX() creates anew object of type X. The input parameters of newX must not include the
 *       object X. To create a new object X from a parent prototype of the same type, use withX
 *       instead.
 *   <li>X withX() to create a new object with a single field set. This is useful for setting fields
 *       within an immutable object.
 *   <li>void setX() to set a field in a mutable object
 *   <li>boolean isX() for validation that returns boolean
 *   <li>void assertX() throws Exception for validations that throws exception
 * </ol>
 */
public final class NodeUtils {

  private NodeUtils() {}

  public static String getHostAndPortString(Node node) {
    HostAndPort hostAndPort = HostAndPort.fromParts(node.getHost(), node.getPort());
    return hostAndPort.toString();
  }

  public static Node newNode(String id) {
    HostAndPort hostAndPort = HostAndPort.fromString(id);
    return Node.newBuilder().setHost(hostAndPort.getHost()).setPort(hostAndPort.getPort()).build();
  }
}
