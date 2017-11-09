package org.corfudb.protocols.wireprotocol.orchestrator;

import lombok.Getter;
import org.corfudb.format.Types;

import java.nio.ByteBuffer;
import java.util.UUID;

import static org.corfudb.format.Types.OrchestratorResponseType.WORKFLOW_STATUS;

/**
 * Return a boolean that indicates whether a particular workflow is being executed by
 * an orchestrator service.
 * @author Maithem
 */
public class QueryResponse implements Response {

    @Getter
    final boolean active;

    public QueryResponse(boolean active) {
        this.active = active;
    }

    public QueryResponse(byte[] buf) {
        if (buf[0] == 0) {
            this.active = false;
        } else {
            this.active = true;
        }
    }

    @Override
    public Types.OrchestratorResponseType getType() {
        return WORKFLOW_STATUS;
    }

    @Override
    public byte[] getSerialized() {
        byte[] buf = new byte[1];
        if (active) {
            buf[0] = 1;
        } else {
            buf[0] = 0;
        }
        return buf;
    }
}