package com.hmdp.config;

import com.hmdp.entity.Shop;
import com.hmdp.service.IShopService;
import lombok.extern.slf4j.Slf4j;
import org.redisson.api.RBloomFilter;
import org.redisson.api.RedissonClient;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;

import static com.hmdp.utils.RedisConstants.BLOOM_SHOP_KEY;

/**
 * 启动时自动初始化商铺布隆过滤器并预热逻辑过期缓存
 */
@Slf4j
@Component
public class ShopCacheWarmUpRunner implements ApplicationRunner {
    @Resource
    private IShopService shopService;
    @Resource
    private RedissonClient redissonClient;
    @Override
    public void run(ApplicationArguments args) throws Exception {
        List<Shop> shops = shopService.list();
        // 布隆过滤器：容量按当前数据量 2 倍预留，误判率 1%
        RBloomFilter<String> bloomFilter = redissonClient.getBloomFilter(BLOOM_SHOP_KEY);
        bloomFilter.tryInit(Math.max(shops.size()*2L,1000L),0.01);
        for (Shop shop : shops) {
            bloomFilter.add(String.valueOf(shop.getId() ));
        }
        // 全量预热逻辑过期缓存
        shopService.warmUpAllShop();
        log.info("商铺缓存预热完成，共 {} 家", shops.size());
    }
}
