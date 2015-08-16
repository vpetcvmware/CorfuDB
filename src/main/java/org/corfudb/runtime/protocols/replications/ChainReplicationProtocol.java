package org.corfudb.runtime.protocols.replications;

import org.corfudb.infrastructure.thrift.Hints;
import org.corfudb.runtime.*;
import org.corfudb.runtime.protocols.IServerProtocol;
import org.corfudb.runtime.protocols.logunits.IWriteOnceLogUnit;
import org.corfudb.runtime.smr.MultiCommand;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.json.JsonArray;
import javax.json.JsonObject;
import javax.json.JsonString;
import javax.json.JsonValue;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.ObjectOutput;
import java.io.ObjectOutputStream;
import java.util.*;
import java.util.regex.Matcher;

/**
 * Created by taia on 8/4/15.
 */
public class ChainReplicationProtocol implements IReplicationProtocol {
    private static final Logger log = LoggerFactory.getLogger(ChainReplicationProtocol.class);
    private List<List<IServerProtocol>> groups = null;

    public ChainReplicationProtocol(List<List<IServerProtocol>> groups) {
        this.groups = groups;
    }

    public static String getProtocolString()
    {
        return "cdbcr";
    }

    public static Map<String, Object> segmentParser(JsonObject jo) {
        Map<String, Object> ret = new HashMap<String, Object>();
        ArrayList<Map<String, Object>> groupList = new ArrayList<Map<String, Object>>();
        for (JsonValue j2 : jo.getJsonArray("groups"))
        {
            HashMap<String,Object> groupItem = new HashMap<String,Object>();
            JsonArray ja = (JsonArray) j2;
            ArrayList<String> group = new ArrayList<String>();
            for (JsonValue j3 : ja)
            {
                group.add(((JsonString)j3).getString());
            }
            groupItem.put("nodes", group);
            groupList.add(groupItem);
        }
        ret.put("groups", groupList);

        return ret;
    }

    public static IReplicationProtocol initProtocol(Map<String, Object> fields,
                                                    Map<String, Class<? extends IServerProtocol>> availableLogUnitProtocols,
                                                    Long epoch) {
        return new ChainReplicationProtocol(populateGroupsFromList((List<Map<String,Object>>) fields.get("groups"), availableLogUnitProtocols, epoch));
    }

    @Override
    public void write(long address, Set<String> streams, byte[] data)
            throws OverwriteException, TrimmedException, OutOfSpaceException, NetworkException {
        int mod = groups.size();
        int groupnum =(int) (address % mod);
        List<IServerProtocol> chain = groups.get(groupnum);
        //writes have to go to chain in order
        long mappedAddress = address/mod;
        for (IServerProtocol unit : chain)
        {
            ((IWriteOnceLogUnit)unit).write(mappedAddress, streams, data);
        }
        return;

    }

    @Override
    public byte[] read(long address, String stream) throws UnwrittenException, TrimmedException, NetworkException {
        int mod = groups.size();
        int groupnum =(int) (address % mod);
        long mappedAddress = address/mod;

        List<IServerProtocol> chain = groups.get(groupnum);
        //reads have to come from last unit in chain
        IWriteOnceLogUnit wolu = (IWriteOnceLogUnit) chain.get(chain.size() - 1);
        return wolu.read(mappedAddress, stream);
    }

    @Override
    public Hints readHints(long address) throws UnwrittenException, TrimmedException, NetworkException {
        // Hints are not chain-replicated; they live at the tails of the chains, where the regular reads go.
        int mod = groups.size();
        int groupnum =(int) (address % mod);
        long mappedAddress = address/mod;

        List<IServerProtocol> chain = groups.get(groupnum);
        IWriteOnceLogUnit wolu = (IWriteOnceLogUnit) chain.get(chain.size() - 1);
        return wolu.readHints(mappedAddress);
    }

    public void setHintsNext(long address, String stream, long nextOffset) throws UnwrittenException, TrimmedException, NetworkException {
        // Hints are not chain-replicated; they live at the tails of the chains, where the regular reads go.
        int mod = groups.size();
        int groupnum =(int) (address % mod);
        long mappedAddress = address/mod;

        List<IServerProtocol> chain = groups.get(groupnum);
        IWriteOnceLogUnit wolu = (IWriteOnceLogUnit) chain.get(chain.size() - 1);
        wolu.setHintsNext(mappedAddress, stream, nextOffset);
    }

    public void setHintsTxDec(long address, boolean dec) throws UnwrittenException, TrimmedException, NetworkException {
        // Hints are not chain-replicated; they live at the tails of the chains, where the regular reads go.
        int mod = groups.size();
        int groupnum =(int) (address % mod);
        long mappedAddress = address/mod;

        List<IServerProtocol> chain = groups.get(groupnum);
        IWriteOnceLogUnit wolu = (IWriteOnceLogUnit) chain.get(chain.size() - 1);
        wolu.setHintsTxDec(mappedAddress, dec);
    }

    public void setHintsFlatTxn(long address, MultiCommand flattedTxn) throws UnwrittenException, TrimmedException, IOException, NetworkException {
        // Hints are not chain-replicated; they live at the tails of the chains, where the regular reads go.
        int mod = groups.size();
        int groupnum =(int) (address % mod);
        long mappedAddress = address/mod;

        // Convert the stream set to a String set
        Set<String> streams = new HashSet<String>();
        if (flattedTxn.getStreams() != null) {
            Iterator<UUID> it = flattedTxn.getStreams().iterator();
            while (it.hasNext()) {
                streams.add(it.next().toString());
            }
        }

        List<IServerProtocol> chain = groups.get(groupnum);
        IWriteOnceLogUnit wolu = (IWriteOnceLogUnit) chain.get(chain.size() - 1);
        try (ByteArrayOutputStream bs = new ByteArrayOutputStream()) {
            try (ObjectOutput out = new ObjectOutputStream(bs)) {
                out.writeObject(flattedTxn);


                wolu.setHintsFlatTxn(mappedAddress, streams, bs.toByteArray());
            }
        }
    }

    @Override
    public List<List<IServerProtocol>> getGroups() {
        return groups;
    }

    @SuppressWarnings("unchecked")
    private static List<List<IServerProtocol>> populateGroupsFromList(List<Map<String,Object>> list,
                                        Map<String,Class<? extends IServerProtocol>> availableLogUnitProtocols,
                                        long epoch) {
        ArrayList<List<IServerProtocol>> groups = new ArrayList<List<IServerProtocol>>();
        for (Map<String,Object> map : list)
        {
            ArrayList<IServerProtocol> nodes = new ArrayList<IServerProtocol>();
            for (String node : (List<String>)map.get("nodes"))
            {
                Matcher m = IServerProtocol.getMatchesFromServerString(node);
                if (m.find())
                {
                    String protocol = m.group("protocol");
                    if (!availableLogUnitProtocols.keySet().contains(protocol))
                    {
                        log.warn("Unsupported logunit protocol: " + protocol);
                    }
                    else
                    {
                        Class<? extends IServerProtocol> sprotocol = availableLogUnitProtocols.get(protocol);
                        try
                        {
                            nodes.add(IServerProtocol.protocolFactory(sprotocol, node, epoch));
                        }
                        catch (Exception ex){
                            log.error("Error invoking protocol for protocol: ", ex);
                        }
                    }
                }
                else
                {
                    log.warn("Logunit string " + node + " appears to be an invalid logunit string");
                }
            }
            groups.add(nodes);
        }
        return groups;
    }
}