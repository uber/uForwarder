package com.uber.data.kafka.datatransfer.management;

import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import org.springframework.boot.actuate.endpoint.annotation.ReadOperation;
import org.springframework.boot.actuate.endpoint.web.annotation.WebEndpoint;

@WebEndpoint(id = "scalarSnapshotJson")
public class ScalarSnapshotJson {
  private final JsonFormat.Printer jsonPrinter;
  private final Scalar scalar;

  public ScalarSnapshotJson(Scalar scalar, JsonFormat.TypeRegistry typeRegistry) {
    this.scalar = scalar;
    this.jsonPrinter =
        JsonFormat.printer()
            .usingTypeRegistry(typeRegistry)
            .omittingInsignificantWhitespace()
            .includingDefaultValueFields();
  }

  @ReadOperation
  String read() throws Exception {
    return jsonPrinter.print(scalar.snapshot());
  }
}
