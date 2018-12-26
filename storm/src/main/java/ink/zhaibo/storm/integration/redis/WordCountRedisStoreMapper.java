package ink.zhaibo.storm.integration.redis;

import org.apache.storm.redis.common.mapper.RedisDataTypeDescription;
import org.apache.storm.redis.common.mapper.RedisStoreMapper;
import org.apache.storm.tuple.ITuple;

/**
 * 存储
 */
public class WordCountRedisStoreMapper implements RedisStoreMapper {
    /*hget wordCount a*/
    private RedisDataTypeDescription description;
    private final String hashKey = "wordCount";

    public WordCountRedisStoreMapper() {
        description = new RedisDataTypeDescription(
                RedisDataTypeDescription.RedisDataType.HASH, hashKey);
    }

    public RedisDataTypeDescription getDataTypeDescription() {
        return description;
    }

    public String getKeyFromTuple(ITuple tuple) {
        return tuple.getStringByField("word");
    }

    public String getValueFromTuple(ITuple tuple) {
        return tuple.getStringByField("count");
    }
}
