package com.hmdp.utils;

public class RedisConstants {
    public static final String LOGIN_CODE_KEY = "login:code:";
    public static final Long LOGIN_CODE_TTL = 2L;
    public static final String LOGIN_USER_KEY = "login:token:";
    public static final Long LOGIN_USER_TTL = 36000L;

    public static final Long CACHE_NULL_TTL = 2L;

    public static final Long CACHE_SHOP_TTL = 30L;
    public static final String CACHE_SHOP_KEY = "cache:shop:";

    public static final String LOCK_SHOP_KEY = "lock:shop:";
    public static final Long LOCK_SHOP_TTL = 10L;

    public static final String SECKILL_STOCK_KEY = "seckill:stock:";
    // 一人一单标记，存储已下单用户 id 的 Set
    public static final String SECKILL_ORDER_KEY = "seckill:order:";
    // 活动开始/结束时间（秒级时间戳），供 Lua 脚本做原子时间校验
    public static final String SECKILL_BEGIN_KEY = "seckill:begin:";
    public static final String SECKILL_END_KEY = "seckill:end:";
    public static final String SECKILL_STREAM_KEY = "stream.orders";
    // 秒杀死信队列：重试耗尽的订单消息留痕，供人工排查
    public static final String SECKILL_DEAD_STREAM_KEY = "stream.orders.dead";
    public static final String BLOG_LIKED_KEY = "blog:liked:";
    public static final String FEED_KEY = "feed:";
    public static final String SHOP_GEO_KEY = "shop:geo:";
    public static final String USER_SIGN_KEY = "sign:";
    public static final String BLOOM_SHOP_KEY = "bloom:shop:";
}
