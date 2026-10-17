package com.hmdp.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;

import java.util.concurrent.ThreadPoolExecutor;

/**
 * 缓存重建专用线程池配置，交由 Spring 容器管理生命周期。
 */
@Configuration
public class CacheThreadPoolConfig {
    /**
     * 缓存击穿后异步重建缓存专用的线程池。
     * <p>
     * 重建任务与请求线程解耦，保证「热点 key 失效时只有线程池里的任务在查库重建，
     * 其余请求立即返回旧数据」，不阻塞用户请求线程。生命周期由 Spring 容器托管，
     * 使用方按 bean 名注入：{@code @Resource(name = "cacheExecutor")}。
     * <p>
     * 注意：当前 {@code CacheClient} 仍使用自身的 static 线程池，尚未切换到本 Bean。
     */
    @Bean("cacheExecutor")
    public ThreadPoolTaskExecutor cacheExecutor(){
        ThreadPoolTaskExecutor executor = new ThreadPoolTaskExecutor();
        // 常驻线程数：按重建任务的 IO 等待型特征设置，避免频繁创建销毁
        executor.setCorePoolSize(10);
        // 突发流量下允许扩容到的上限，队列打满后才创建非核心线程
        executor.setMaxPoolSize(20);
        // 有界队列：故意不设无界队列（Executors 默认值），防止任务堆积拖垮内存
        executor.setQueueCapacity(100);
        // 非核心线程空闲回收时间，峰值过后把线程数降回 corePoolSize
        executor.setKeepAliveSeconds(60);
        // 统一线程名前缀，便于日志和 jstack dump 中把线程归因到缓存重建
        executor.setThreadNamePrefix("cache-rebuild-");
        // 拒绝策略：队列满时由调用线程自己执行，起到背压作用，避免任务被直接丢弃
        executor.setRejectedExecutionHandler(new ThreadPoolExecutor.CallerRunsPolicy());
        // 优雅停机：关闭容器时先停止接收新任务，并等待已提交任务执行完毕，
        // 最长等 30 秒后再强制关闭，避免线程池里还有非守护任务时 JVM 无法正常退出
        executor.setWaitForTasksToCompleteOnShutdown(true);
        executor.setAwaitTerminationSeconds(30);
        // 手动触发初始化（内部实现 InitializingBean，Spring 回调时也会调一次，这里显式写出保证提前可用）
        executor.initialize();
        return executor;
    }
}
