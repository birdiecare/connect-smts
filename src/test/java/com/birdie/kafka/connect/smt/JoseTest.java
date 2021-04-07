package com.birdie.kafka.connect.smt;

import org.apache.kafka.connect.data.SchemaBuilder;
import org.apache.kafka.connect.source.SourceRecord;
import org.junit.After;
import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.*;

public class JoseTest {

  private Jose<SourceRecord> transformer = new Jose.DecryptValue<>();

  @After
  public void tearDown() throws Exception {
    transformer.close();
  }

  @Test
  public void decryptEncryptedMessage() {
    final Map<String, String> props = new HashMap<String, String>() {{
      put("keys", "eyJrdHkiOiJvY3QiLCJraWQiOiJIMmg1WHlIM0dPSHQ4Wm5vTndBOUFoay1FQlJ3cTQxTHo3U01pWDJDLVRJIiwiYWxnIjoiQTI1NkdDTSIsImsiOiJkR2tQRU41YUhaQnFocDY5TmpVWGpjTUJST1R4amZ6Yl9hYldVeUo2ajBBIn0=");
    }};

    transformer.configure(props);

    String encryptedContent = "{\"protected\":\"eyJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiZGlyIiwia2lkIjoiSDJoNVh5SDNHT0h0OFpub053QTlBaGstRUJSd3E0MUx6N1NNaVgyQy1USSJ9\",\"iv\":\"QsJ90lUJoBW05RN3\",\"ciphertext\":\"4YQYK0F4LoFpP6-P0g\",\"tag\":\"88OtNkM7CVsOX52rLxgl2Q\"}";

    final SourceRecord record = new SourceRecord(null, null, "test", 0, SchemaBuilder.bytes().optional().build(), "key".getBytes(), SchemaBuilder.bytes().optional().build(), encryptedContent.getBytes());
    final SourceRecord transformedRecord = transformer.apply(record);

    assertTrue(transformedRecord.value() instanceof byte[]);
    String stringified = new String((byte[]) transformedRecord.value());
    assertEquals("{\"foo\":\"bar\"}", stringified);
  }

  @Test
  public void decryptWithMultipleKeys() {
    final Map<String, String> props = new HashMap<String, String>() {{
      put("keys", "eyJrdHkiOiJvY3QiLCJraWQiOiJIMmg1WHlIM0dPSHQ4Wm5vTndBOUFoay1FQlJ3cTQxTHo3U01pWDJDLVRJIiwiYWxnIjoiQTI1NkdDTSIsImsiOiJkR2tQRU41YUhaQnFocDY5TmpVWGpjTUJST1R4amZ6Yl9hYldVeUo2ajBBIn0=,eyJrdHkiOiJvY3QiLCJraWQiOiJjbmVkYXRXNEg4SFA4VjFFSVQ3QjJsdHk0TXFsM3FKV3ZPUmk1MFZVdWZvIiwiYWxnIjoiQTI1NkdDTSIsImsiOiJjYmtncEtVZ1c0RFRaS2RkQzAwcHJTSjl5UnZGSGc3OTFOa0V6R2RLTFlZIn0=");
    }};

    transformer.configure(props);

    String encryptedContent = "{\"protected\":\"eyJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiZGlyIiwia2lkIjoiY25lZGF0VzRIOEhQOFYxRUlUN0IybHR5NE1xbDNxSld2T1JpNTBWVXVmbyJ9\",\"iv\":\"rhGttLfHmwq7beFr\",\"ciphertext\":\"L4OQkrC1zagy2DLDtw\",\"tag\":\"9T29uivX5K_O_503UKmicA\"}";

    final SourceRecord record = new SourceRecord(null, null, "test", 0, SchemaBuilder.bytes().optional().build(), "key".getBytes(), SchemaBuilder.bytes().optional().build(), encryptedContent.getBytes());
    final SourceRecord transformedRecord = transformer.apply(record);

    assertTrue(transformedRecord.value() instanceof byte[]);
    String stringified = new String((byte[]) transformedRecord.value());
    assertEquals("{\"bar\":\"baz\"}", stringified);
  }

  @Test(expected = IllegalArgumentException.class)
  public void failIfCannotDecryptByDefault() {
    final Map<String, String> props = new HashMap<String, String>() {{
      put("keys", "eyJrdHkiOiJvY3QiLCJraWQiOiJIMmg1WHlIM0dPSHQ4Wm5vTndBOUFoay1FQlJ3cTQxTHo3U01pWDJDLVRJIiwiYWxnIjoiQTI1NkdDTSIsImsiOiJkR2tQRU41YUhaQnFocDY5TmpVWGpjTUJST1R4amZ6Yl9hYldVeUo2ajBBIn0=");
    }};

    transformer.configure(props);

    String encryptedContent = "{\"protected\":\"eyJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiZGlyIiwia2lkIjoiY25lZGF0VzRIOEhQOFYxRUlUN0IybHR5NE1xbDNxSld2T1JpNTBWVXVmbyJ9\",\"iv\":\"rhGttLfHmwq7beFr\",\"ciphertext\":\"L4OQkrC1zagy2DLDtw\",\"tag\":\"9T29uivX5K_O_503UKmicA\"}";

    final SourceRecord record = new SourceRecord(null, null, "test", 0, SchemaBuilder.bytes().optional().build(), "key".getBytes(), SchemaBuilder.bytes().optional().build(), encryptedContent.getBytes());
    transformer.apply(record);
  }

  @Test()
  public void ignoresDecryptionFailureIfConfiguredAndReturnsOriginalPayload() {
    final Map<String, Object> props = new HashMap<String, Object>() {{
      put("keys", "eyJrdHkiOiJvY3QiLCJraWQiOiJIMmg1WHlIM0dPSHQ4Wm5vTndBOUFoay1FQlJ3cTQxTHo3U01pWDJDLVRJIiwiYWxnIjoiQTI1NkdDTSIsImsiOiJkR2tQRU41YUhaQnFocDY5TmpVWGpjTUJST1R4amZ6Yl9hYldVeUo2ajBBIn0=");
      put("skip-on-error", true);
    }};

    transformer.configure(props);

    String encryptedContent = "{\"protected\":\"eyJlbmMiOiJBMjU2R0NNIiwiYWxnIjoiZGlyIiwia2lkIjoiY25lZGF0VzRIOEhQOFYxRUlUN0IybHR5NE1xbDNxSld2T1JpNTBWVXVmbyJ9\",\"iv\":\"rhGttLfHmwq7beFr\",\"ciphertext\":\"L4OQkrC1zagy2DLDtw\",\"tag\":\"9T29uivX5K_O_503UKmicA\"}";

    final SourceRecord record = new SourceRecord(null, null, "test", 0, SchemaBuilder.bytes().optional().build(), "key".getBytes(), SchemaBuilder.bytes().optional().build(), encryptedContent.getBytes());
    final SourceRecord transformedRecord = transformer.apply(record);

    assertEquals(encryptedContent, new String((byte[]) transformedRecord.value()));
  }

  @Test()
  public void ignoresParsingContentIfConfiguredAndReturnsPayload() {
    final Map<String, Object> props = new HashMap<String, Object>() {{
      put("keys", "eyJrdHkiOiJvY3QiLCJraWQiOiJIMmg1WHlIM0dPSHQ4Wm5vTndBOUFoay1FQlJ3cTQxTHo3U01pWDJDLVRJIiwiYWxnIjoiQTI1NkdDTSIsImsiOiJkR2tQRU41YUhaQnFocDY5TmpVWGpjTUJST1R4amZ6Yl9hYldVeUo2ajBBIn0=");
      put("skip-on-error", true);
    }};

    transformer.configure(props);

    String encryptedContent = "Not a JWE format at all.";

    final SourceRecord record = new SourceRecord(null, null, "test", 0, SchemaBuilder.bytes().optional().build(), "key".getBytes(), SchemaBuilder.bytes().optional().build(), encryptedContent.getBytes());
    final SourceRecord transformedRecord = transformer.apply(record);

    assertEquals(encryptedContent, new String((byte[]) transformedRecord.value()));
  }
}
