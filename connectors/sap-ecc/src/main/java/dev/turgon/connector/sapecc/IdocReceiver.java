package dev.turgon.connector.sapecc;

import java.util.List;
import java.util.Map;

/**
 * Receives IDocs SAP sends to the registered server program
 * (IDOC_INBOUND_ASYNCHRONOUS over tRFC): the control records (EDI_DC40)
 * and data records (EDI_DD40) of one transaction. Returning confirms the
 * transaction to SAP; throwing fails it, and SAP sends it again (SM58).
 */
@FunctionalInterface
public interface IdocReceiver {
    void receive(String tid, List<Map<String, String>> control, List<Map<String, String>> data) throws Exception;
}
