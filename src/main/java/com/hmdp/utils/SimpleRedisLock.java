package com.hmdp.utils;

import cn.hutool.core.lang.UUID;
import org.springframework.data.redis.core.StringRedisTemplate;

import java.util.concurrent.TimeUnit;

public class SimpleRedisLock implements Ilock{
    private String name;
    private StringRedisTemplate stringRedisTemplate;
    private static final String KEY_PREFIX="lock:";
    private static final String ID_PREFIX= UUID.randomUUID().toString(true)+"-";
    public SimpleRedisLock(String name, StringRedisTemplate stringRedisTemplate) {
        this.name = name;
        this.stringRedisTemplate = stringRedisTemplate;
    }
    @Override
    public boolean tryLock(long timeoutSec) {
        //获取线程标识
        String threadId = ID_PREFIX+Thread.currentThread().getId();
        // 获取锁
        Boolean success = stringRedisTemplate.opsForValue().setIfAbsent(KEY_PREFIX + name, threadId , timeoutSec, TimeUnit.SECONDS);

        return Boolean.TRUE.equals(success);
    }

    @Override
    public void unLock() {
        //获取线程标识
        String threadId = ID_PREFIX+Thread.currentThread().getId();
        //获取锁中的线程标识
        String id = stringRedisTemplate.opsForValue().get(KEY_PREFIX + name);
        //判断
        if(threadId.equals(id)){
            stringRedisTemplate.delete(KEY_PREFIX + name);
        }


    }
}
