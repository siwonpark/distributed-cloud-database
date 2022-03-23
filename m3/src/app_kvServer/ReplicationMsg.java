package app_kvServer;

public class ReplicationMsg {
    
    enum ReplicationMsgType{
		REPLICATE_MIDDLE_REPLICA,
		REPLICATE_TAIL,
		ACK_FROM_MIDDLE_REPLICA,
		ACK_FROM_TAIL
	}

    ReplicationMsg(String k, String v, long seq, ReplicationMsgType type){
        this.key = k;
        this.value = v;
        this.sequence = seq;
        this.type = type;
    }
    private static long globalSeq = 0;
    public static void increSeq(){
        ReplicationMsg.globalSeq += 1;
    }
    public static long getSeq(){
        return ReplicationMsg.globalSeq;
    }
    String key;
    String value;
    long sequence;
    ReplicationMsgType type;
}
