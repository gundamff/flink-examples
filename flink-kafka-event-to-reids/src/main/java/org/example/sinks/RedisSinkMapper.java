package org.example.sinks;

import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommand;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisCommandDescription;
import org.apache.flink.streaming.connectors.redis.common.mapper.RedisMapper;
import org.example.model.DataEvent;

public class RedisSinkMapper implements RedisMapper<DataEvent> {
    @Override
    public RedisCommandDescription getCommandDescription() {
        return new RedisCommandDescription(RedisCommand.SET);
    }

    @Override
    public String getKeyFromData(DataEvent data) {
        System.out.println("data.getKeyFromData" + data);
        return String.format("%s:%s:%s", data.getId());
    }

    @Override
    public String getValueFromData(DataEvent data) {
        return String.valueOf(data.getValue());
    }
}