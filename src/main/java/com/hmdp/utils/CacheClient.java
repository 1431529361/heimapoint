package com.hmdp.utils;

import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.RandomUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;

import java.util.concurrent.TimeUnit;
import java.util.function.Function;

import static com.hmdp.utils.RedisConstants.CACHE_NULL_TTL;
import static com.hmdp.utils.RedisConstants.LOCK_SHOP_KEY;

/**
 * 缓存工具类，封装了常用的 Redis 缓存操作策略。
 * 包括缓存穿透（空值缓存）、缓存击穿（互斥锁、逻辑过期）等解决方案。
 */
@Slf4j
@Component
public class CacheClient {
    private final StringRedisTemplate stringRedisTemplate;
    private final ThreadPoolTaskExecutor cacheExecutor;
    public CacheClient(StringRedisTemplate stringRedisTemplate, @Qualifier("cacheExecutor") ThreadPoolTaskExecutor cacheExecutor) {
        this.stringRedisTemplate = stringRedisTemplate;
        this.cacheExecutor = cacheExecutor;
    }



    /**
     * 将任意对象序列化为 JSON 并写入 Redis，设置过期时间。
     *
     * @param key   Redis 键
     * @param value 要缓存的对象
     * @param time  过期时间数值
     * @param unit  过期时间单位
     */
    public void set(String key, Object value, Long time, TimeUnit  unit){
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(value),time,unit);
    }

    /**
     * 将对象写入 Redis，并附带逻辑过期时间（不设置 Redis 物理过期）。
     * 数据会被包装在 RedisData 中，包含 expireTime 字段用于判断逻辑过期。
     *
     * @param key   Redis 键
     * @param value 要缓存的对象
     * @param time  逻辑过期时间数值
     * @param unit  逻辑过期时间单位
     */
    public void setWithLogicalExpire(String key, Object value, Long time, TimeUnit  unit){
        RedisData redisData = new RedisData();
        redisData.setData(value);
        long seconds = unit.toSeconds(time);
        long jitter = RandomUtil.randomLong(0,Math.max(seconds/10,1));
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(seconds + jitter));
        stringRedisTemplate.opsForValue().set(key, JSONUtil.toJsonStr(redisData));
    }

    /**
     * 缓存穿透解决方案：查询时若缓存未命中则查数据库，并将结果写入缓存。
     * 若数据库也不存在，则缓存空字符串，防止恶意请求穿透到数据库。
     *
     * @param keyPrefix Redis 键前缀
     * @param id        查询的 id
     * @param type      返回对象的类型
     * @param dbFallback 缓存未命中时查询数据库的逻辑
     * @param time      缓存过期时间数值
     * @param unit      缓存过期时间单位
     * @param <R>       返回类型
     * @param <ID>      id 类型
     * @return 查询结果，不存在返回 null
     */
    public <R,ID> R queryWithPassThrough(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        if (StrUtil.isNotBlank(json)) {
            return JSONUtil.toBean(json, type);
        }
        // json 不为 null 但为空字符串，说明是之前缓存的空值（缓存穿透防护）
        if (json != null) {
            return null;
        }
        R r = dbFallback.apply(id);
        if (r == null) {
            // 数据库也不存在，缓存空值防止穿透
            stringRedisTemplate.opsForValue().set(key, "",CACHE_NULL_TTL,TimeUnit.MINUTES);
            return null;
        }
        this.set(key, r, time, unit);
        return r;
    }

    /**
     * 逻辑过期方案解决缓存击穿：缓存不设置物理过期时间，通过逻辑过期字段判断是否需要重建。
     * 过期时异步重建缓存，当前请求仍返回旧数据，保证高可用。
     *
     * @param keyPrefix Redis 键前缀
     * @param id        查询的 id
     * @param type      返回对象的类型
     * @param dbFallback 缓存未命中时查询数据库的逻辑
     * @param time      逻辑过期时间数值
     * @param unit      逻辑过期时间单位
     * @param <R>       返回类型
     * @param <ID>      id 类型
     * @return 查询结果，缓存未命中返回 null
     */
    public <R,ID> R queryWithLogicalExpire(String keyPrefix, ID id, Class<R> type, Function<ID,R> dbFallback, Long time, TimeUnit unit){
        String key = keyPrefix + id;
        String json = stringRedisTemplate.opsForValue().get(key);
        // 缓存未命中，直接返回 null（逻辑过期方案需提前预热缓存）
        if (StrUtil.isBlank(json)) {
            return null;
        }
        // 命中缓存，反序列化为 RedisData 包装对象
        RedisData redisData = JSONUtil.toBean(json, RedisData.class);
        R r = JSONUtil.toBean((JSONObject) redisData.getData(), type);
        LocalDateTime expireTime = redisData.getExpireTime();
        // 判断逻辑过期时间
        if(expireTime.isAfter(LocalDateTime.now())) {
            return r;
        }
        // 已过期，尝试获取互斥锁进行缓存重建
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if(isLock){
            // 双重检查：获取锁后再次验证缓存是否已被其他线程重建
            String shopJson = stringRedisTemplate.opsForValue().get(key);
            if(StrUtil.isBlank(shopJson)){
                unlock(lockKey);
                return null;
            }
            RedisData latestData = JSONUtil.toBean(shopJson, RedisData.class);
            LocalDateTime expireTime2 = latestData.getExpireTime();
            if(expireTime2.isAfter(LocalDateTime.now())){
                unlock(lockKey);
                return JSONUtil.toBean((JSONObject) latestData.getData(), type);
            }
            // 确认过期，异步重建缓存
            cacheExecutor.submit(()->{
                try{
                    R r1 = dbFallback.apply(id);
                    this.setWithLogicalExpire(key, r1, time, unit);
                }catch (Exception e){
                    // 后台线程里抛 RuntimeException 无人接管，改为记录日志，保证可观测
                    log.error("缓存重建失败,key={}",key,e);
                }finally {
                    unlock(lockKey);
                }
            });
        }
        // 重建期间返回旧数据
        return r;
    }

    /**
     * 互斥锁方案解决缓存击穿：缓存未命中时获取互斥锁重建缓存。
     * 获取锁失败的线程休眠后重试，保证只有一个线程查询数据库。
     * 同时支持缓存穿透防护（缓存空值）。
     *
     * @param keyPrefix Redis 键前缀
     * @param id        查询的 id
     * @param type      返回对象的类型
     * @param dbFallback 缓存未命中时查询数据库的逻辑
     * @param time      缓存过期时间数值
     * @param unit      缓存过期时间单位
     * @param <R>       返回类型
     * @param <ID>      id 类型
     * @return 查询结果，不存在返回 null
     */
    public <R, ID> R queryWithMutex(
            String keyPrefix, ID id, Class<R> type, Function<ID, R> dbFallback, Long time, TimeUnit unit) {
        String key = keyPrefix + id;
        String shopJson = stringRedisTemplate.opsForValue().get(key);
        // 缓存命中且不为空值
        if (StrUtil.isNotBlank(shopJson)) {
            return JSONUtil.toBean(shopJson, type);
        }
        // 缓存为空字符串，说明是穿透防护的空值
        if (shopJson != null) {
            return null;
        }

        // 缓存未命中，尝试获取互斥锁重建缓存
        String lockKey = LOCK_SHOP_KEY + id;
        R r = null;
        try {
            boolean isLock = tryLock(lockKey);
            if (!isLock) {
                // 获取锁失败，休眠 50ms 后递归重试
                Thread.sleep(50);
                return queryWithMutex(keyPrefix, id, type, dbFallback, time, unit);
            }
            // 获取锁成功，双重检查缓存是否已被其他线程重建
            shopJson = stringRedisTemplate.opsForValue().get(key);
            if (StrUtil.isNotBlank(shopJson)) {
                return JSONUtil.toBean(shopJson, type);
            }
            if (shopJson != null) {
                return null;
            }
            // 查询数据库
            r = dbFallback.apply(id);
            if (r == null) {
                // 数据库不存在，缓存空值防止穿透
                stringRedisTemplate.opsForValue().set(key, "", CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            this.set(key, r, time, unit);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unlock(lockKey);
        }
        return r;
    }

    /**
     * 尝试获取互斥锁（基于 Redis setIfAbsent 实现）。
     *
     * @param key 锁的 Redis 键
     * @return true 获取成功，false 获取失败
     */
    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    /**
     * 释放互斥锁（删除 Redis 中对应的键）。
     *
     * @param key 锁的 Redis 键
     */
    private void unlock(String key){
        stringRedisTemplate.delete(key);
    }



}
