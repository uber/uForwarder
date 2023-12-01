package com.uber.data.kafka.datatransfer.common;

import java.io.UnsupportedEncodingException;
import javax.annotation.Nullable;
import org.I0Itec.zkclient.exception.ZkMarshallingError;
import org.I0Itec.zkclient.serialize.ZkSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * ZKStringSerializer is an utility class comes with kafka ZkUtils. the class should be removed on
 * Kafka version > 2.4
 *
 * @deprecated use Kafka2.4 KafkaZkClient API instead
 */
@Deprecated
public class ZKStringSerializer implements ZkSerializer {
  private final Logger logger = LoggerFactory.getLogger(ZKStringSerializer.class);

  @Override
  @Nullable
  public byte[] serialize(Object data) throws ZkMarshallingError {
    if (data == null) {
      return null;
    }
    String strData = (String) data;
    try {
      return strData.getBytes("UTF-8");
    } catch (UnsupportedEncodingException e) {
      logger.warn("Unsupported encoding Exception:", e);
      return null;
    }
  }

  @Override
  @Nullable
  public Object deserialize(byte[] bytes) throws ZkMarshallingError {
    if (bytes == null) {
      return null;
    } else {
      try {
        return new String(bytes, "UTF-8");
      } catch (Exception e) {
        logger.warn("Unsupported encoding Exception:", e);
        return null;
      }
    }
  }
}
