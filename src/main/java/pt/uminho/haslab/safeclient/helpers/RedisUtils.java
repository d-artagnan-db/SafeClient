package pt.uminho.haslab.safeclient.helpers;

import redis.clients.jedis.Jedis;

public class RedisUtils {


    public static void flushAll(String hostname) {
        Jedis jedis = new Jedis(hostname);
        jedis.flushDB();
    }
}
