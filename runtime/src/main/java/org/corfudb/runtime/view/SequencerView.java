package org.corfudb.runtime.view;

import java.util.Set;
import java.util.UUID;

import org.corfudb.protocols.wireprotocol.TokenResponse;
import org.corfudb.protocols.wireprotocol.TxResolutionInfo;
import org.corfudb.runtime.CorfuRuntime;
import org.corfudb.runtime.Tracer;
import org.corfudb.util.CFUtils;


/**
 * Created by mwei on 12/10/15.
 */

public class SequencerView extends AbstractView {

    public SequencerView(CorfuRuntime runtime) {
        super(runtime);
    }

    /**
     * Return the next token in the sequencer for a particular stream.
     *
     * <p>If numTokens == 0, then the streamAddressesMap returned is the last handed out token for
     * each stream (if streamIDs is not empty). The token returned is the global address as
     * previously defined, namely, max global address across all the streams.</p>
     *
     * @param streamIDs The stream IDs to retrieve from.
     * @param numTokens The number of tokens to reserve.
     * @return The first token retrieved.
     */
    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens) {
        long ts1 = System.nanoTime();
        try {
            return _nextToken(streamIDs, numTokens);
        } finally {
            long ts2 = System.nanoTime();
            Tracer.getTracer().log("Seq [dur] " + (ts2 - ts1) +" [ids] " + streamIDs + " [num] " + numTokens);
        }
    }

    public TokenResponse _nextToken(Set<UUID> streamIDs, int numTokens) {
        return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                .nextToken(streamIDs, numTokens)));
    }


    public TokenResponse nextToken(Set<UUID> streamIDs, int numTokens,
                                   TxResolutionInfo conflictInfo) {
        long ts1 = System.nanoTime();
        try {
            return _nextToken(streamIDs, numTokens, conflictInfo);
        } finally {
            long ts2 = System.nanoTime();
            Tracer.getTracer().log("SeqTx [dur] " + (ts2 - ts1) +" [ids] " + streamIDs + " [num] " + numTokens);
        }
    }

    public TokenResponse _nextToken(Set<UUID> streamIDs, int numTokens,
                                  TxResolutionInfo conflictInfo) {
        return layoutHelper(e -> CFUtils.getUninterruptibly(e.getPrimarySequencerClient()
                .nextToken(streamIDs, numTokens, conflictInfo)));
    }

    public void trimCache(long address) {
        runtime.getLayoutView().getRuntimeLayout().getPrimarySequencerClient().trimCache(address);
    }
}