package com.uber.data.kafka.datatransfer.management;

import com.google.protobuf.util.JsonFormat;
import com.uber.data.kafka.datatransfer.ScaleStoreSnapshot;
import com.uber.data.kafka.datatransfer.controller.autoscalar.Scalar;
import com.uber.fievel.testing.base.FievelTestBase;
import org.junit.Test;
import org.mockito.Mockito;

public class ScalarSnapshotJsonTest extends FievelTestBase {
  @Test
  public void testRead() throws Exception {
    Scalar scalar = Mockito.mock(Scalar.class);
    Mockito.when(scalar.snapshot()).thenReturn(ScaleStoreSnapshot.newBuilder().build());
    ScalarSnapshotJson scalarSnapshotJson =
        new ScalarSnapshotJson(scalar, JsonFormat.TypeRegistry.getEmptyTypeRegistry());
    scalarSnapshotJson.read();
    Mockito.verify(scalar, Mockito.times(1)).snapshot();
  }
}
