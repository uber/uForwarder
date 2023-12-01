package com.uber.data.kafka.datatransfer.controller;
/**
 * This is the master/controller for the data transfer framework.
 *
 * @implNote we use Spring AutoConfiguration to make the user experience configuration based.
 *     <p>See README for usage details.
 */
public final class DataTransferMaster {
  public static final String SPRING_PROFILE = "data-transfer-controller";
}
