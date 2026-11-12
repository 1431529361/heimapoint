package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.dto.Result;
import com.hmdp.entity.VoucherOrder;
import com.hmdp.mapper.VoucherOrderMapper;
import com.hmdp.service.ISeckillVoucherService;
import com.hmdp.service.IVoucherOrderService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RedisIdWorker;
import com.hmdp.utils.UserHolder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.context.annotation.Lazy;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.domain.Range;
import org.springframework.data.redis.connection.stream.ByteRecord;
import org.springframework.data.redis.connection.stream.Consumer;
import org.springframework.data.redis.connection.stream.MapRecord;
import org.springframework.data.redis.connection.stream.PendingMessage;
import org.springframework.data.redis.connection.stream.PendingMessages;
import org.springframework.data.redis.connection.stream.ReadOffset;
import org.springframework.data.redis.connection.stream.StreamOffset;
import org.springframework.data.redis.connection.stream.StreamReadOptions;
import org.springframework.data.redis.core.RedisCallback;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import javax.annotation.PostConstruct;
import javax.annotation.PreDestroy;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * <p>
 *  优惠券秒杀订单服务实现
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class VoucherOrderServiceImpl extends ServiceImpl<VoucherOrderMapper, VoucherOrder> implements IVoucherOrderService {

    /**
     * 显式声明 SLF4J 日志对象：父类 ServiceImpl 自带一个 ibatis 的 log 字段，
     * 它只能记录 error，这里用同名变量隐藏父类字段，保证三个日志级别都能用
     */
    private static final Logger log = LoggerFactory.getLogger(VoucherOrderServiceImpl.class);

    /** 消费者组名 */
    private static final String GROUP_NAME = "g1";
    /** 消费者名 */
    private static final String CONSUMER_NAME = "c1";
    /**
     * "一人一单"标记 Set 的存活时间。
     * 该 Set 每次下单都会顺延 TTL，活动结束且不再有新订单后自然回收，
     * 避免像原来那样没有过期时间、永久占用内存。
     */
    private static final long SECKILL_ORDER_TTL_SECONDS = 7 * 24 * 60 * 60L;
    /** pending-list 单轮最多补偿处理的消息条数，防止异常消息把消费线程卡死 */
    private static final int PENDING_BATCH_SIZE = 100;
    /** pending-list 巡检间隔（毫秒） */
    private static final long PENDING_CHECK_INTERVAL_MILLIS = 10_000L;
    /**
     * XCLAIM 认领阈值：必须显著大于 BLOCK 2s + 一次 DB 事务耗时，
     * 避免抢走消费线程正在处理的消息造成并发双处理
     */
    private static final Duration CLAIM_MIN_IDLE = Duration.ofSeconds(30);
    /** 投递次数超过该值判定为毒消息，转死信止血；正常消息 1~2 次投递内就该成功 */
    private static final long MAX_DELIVERY_COUNT = 5;

    @Autowired
    private ISeckillVoucherService seckillVoucherService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private RedisIdWorker redisIdWorker;

    /**
     * 注入自身代理：@Transactional 只有在经过 Spring 代理调用时才生效。
     * 后台消费线程里无法使用 AopContext.currentProxy()（它依赖当前线程存在代理调用栈），
     * 所以直接持有自身代理引用；@Lazy 用于打破"自己依赖自己"的循环创建。
     */
    @Autowired
    @Lazy
    private IVoucherOrderService selfProxy;

    private static final DefaultRedisScript<Long> SECKILL_SCRIPT;

    static {
        SECKILL_SCRIPT = new DefaultRedisScript<>();
        SECKILL_SCRIPT.setLocation(new ClassPathResource("seckill.lua"));
        SECKILL_SCRIPT.setResultType(Long.class);
    }

    private static final ExecutorService SECKILL_ORDER_EXECUTOR = Executors.newSingleThreadExecutor();

    @PostConstruct
    private void init() {
        // 消费者组必须显式创建，否则 XREADGROUP 会直接报 NOGROUP。
        // 起始位点用 0 而不是 $，是为了避免重建消费者组时漏掉历史上未被确认的订单，
        // 由此带来的重复投递由 createVoucherOrder 里的"一人一单"查询做幂等兜底。
        createConsumerGroupIfAbsent();
        SECKILL_ORDER_EXECUTOR.submit(new VoucherOrderHandler());
    }
    @PreDestroy
    private void destroy() {
        SECKILL_ORDER_EXECUTOR.shutdownNow();
    }

    /**
     * 创建消费者组，已存在时静默跳过。
     * 使用 XGROUP CREATE 的 MKSTREAM 选项，保证 Stream 尚未产生消息时也能建组。
     */
    private void createConsumerGroupIfAbsent() {
        try {
            stringRedisTemplate.execute((RedisCallback<String>) connection ->
                    connection.xGroupCreate(RedisConstants.SECKILL_STREAM_KEY.getBytes(StandardCharsets.UTF_8),
                            GROUP_NAME, ReadOffset.from("0"), true));
            log.info("秒杀订单消费者组创建成功：" + GROUP_NAME);
        } catch (Exception e) {
            if (containsBusyGroup(e)) {
                log.info("秒杀订单消费者组已存在，跳过创建：" + GROUP_NAME);
            } else {
                // 启动期 Redis 不可用不阻断应用启动，消费线程会在 Redis 恢复后继续工作
                log.error("秒杀订单消费者组创建失败，请检查 Redis 状态", e);
            }
        }
    }

    /**
     * 判断异常链中是否包含 BUSYGROUP（消费者组已存在）错误。
     * Spring Data Redis 会把 Redis 错误包成 RedisSystemException，这里沿 cause 链逐层匹配。
     */
    private boolean containsBusyGroup(Throwable e) {
        return containsError(e,"BUSYGROUP");
    }

    /**
     * 异步订单消费线程。
     * 正常消息与 pending 补偿共用同一个线程：两个线程同时按同一个消费者名读取会重复消费，
     * 单线程顺序处理既避免并发重复，也让库存扣减天然串行。
     */
    private class VoucherOrderHandler implements Runnable {

        @Override
        public void run() {
            long lastPendingCheck = System.currentTimeMillis();
            while (true) {
                try {
                    // 0.每隔一段时间巡检一次 pending-list，处理上次消费未确认的消息
                    if (System.currentTimeMillis() - lastPendingCheck >= PENDING_CHECK_INTERVAL_MILLIS) {
                        handlePendingList();
                        lastPendingCheck = System.currentTimeMillis();
                    }
                    // 1.获取消息队列中的订单信息 XREADGROUP GROUP g1 c1 COUNT 1 BLOCK 2000 STREAMS stream.orders >
                    List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                            Consumer.from(GROUP_NAME, CONSUMER_NAME),
                            StreamReadOptions.empty().count(1).block(Duration.ofSeconds(2)),
                            StreamOffset.create(RedisConstants.SECKILL_STREAM_KEY, ReadOffset.lastConsumed()));
                    // 2.没有新消息时 BLOCK 已经阻塞等待过，直接进入下一轮，避免空转
                    if (list == null || list.isEmpty()) {
                        continue;
                    }
                    handleRecord(list.get(0));
                } catch (Exception e) {
                    if(containsError(e,"NOGROUP")){
                        // 组不存在说明启动期建组失败（Redis 未就绪），就地重建实现自愈，
                        // 否则 XREADGROUP 会永远失败、消息只进不出
                        log.warn("消费者组不存在,尝试重建:" + GROUP_NAME);
                        createConsumerGroupIfAbsent();
                    }else {
                        log.error("处理订单异常", e);
                    }

                    // Redis 抖动时退避 1 秒，避免无间隔重试刷爆日志与 CPU
                    try {
                        Thread.sleep(1000);
                    } catch (InterruptedException interrupted) {
                        Thread.currentThread().interrupt();
                        return;
                    }
                }
            }
        }
    }

    /**
     * 处理单条消息：创建订单后立刻 XACK。
     * 创建订单抛异常时不确认，消息会留在 pending-list 由后续巡检补偿。
     */
    private void handleRecord(MapRecord<String, Object, Object> record) {
        Map<Object, Object> value = record.getValue();
        VoucherOrder voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
        // 1.创建订单：必须走代理对象，否则 @Transactional 因自调用而失效；
        //   失败时直接上抛给 run()/handlePendingList()，不 ACK、不做任何 Redis 侧补偿
        selfProxy.createVoucherOrder(voucherOrder);
        // 2.确认消息 XACK：放在业务调用之外单独兜住，避免"ACK 自身失败"被误判为"业务失败"
        try {
            stringRedisTemplate.opsForStream()
                    .acknowledge(RedisConstants.SECKILL_STREAM_KEY, GROUP_NAME, record.getId());
        } catch (Exception e) {
            log.error("订单已落库但 XACK 失败，消息将由 pending 重投幂等补齐，msgId="
                    + record.getId().getValue(), e);
            throw e;
        }


    }

    /**
     * 兜底补偿 pending-list：消费异常、或消费后进程退出未 ACK 的消息都挂在这里。
     * 与旧实现相比增加了单轮条数上限，不会因为某条"毒消息"陷入无界死循环。
     * 独立巡检任务 patrolPendingList 用 XCLAIM 过户到 c1 名下的消息也会在这里被读走处理。
     */
    private void handlePendingList() {
        for (int i = 0; i < PENDING_BATCH_SIZE; i++) {
            try {
                // 1.读取 pending-list XREADGROUP GROUP g1 c1 COUNT 1 STREAMS stream.orders 0
                List<MapRecord<String, Object, Object>> list = stringRedisTemplate.opsForStream().read(
                        Consumer.from(GROUP_NAME, CONSUMER_NAME),
                        StreamReadOptions.empty().count(1),
                        StreamOffset.create(RedisConstants.SECKILL_STREAM_KEY, ReadOffset.from("0")));
                // 2.为空说明 pending-list 已经清理干净，结束本轮
                if (list == null || list.isEmpty()) {
                    return;
                }
                handleRecord(list.get(0));
                log.info("已补偿处理一条 pending 订单消息");
            } catch (Exception e) {
                // 单条失败立即结束本轮，等待下一个巡检周期再试，避免卡住正常消息消费
                log.error("处理 pending 订单异常", e);
                return;
            }
        }
    }

    /**
     * 独立于消费线程的 pending-list 巡检任务：消费循环卡死时兜底照常运行。
     * 只做三件事：XPENDING 查房（只读元数据）、XCLAIM 过户、死信收尸，绝不亲自执行业务——
     * 业务处理仍由单线程消费循环完成，保持全局串行，
     * 避免 createVoucherOrder 的 count 幂等查询在并发下竞态（订单表没有唯一索引兜底）。
     */
    @Scheduled(cron = "0/5 * * * * ?")
    public void patrolPendingList() {
        try {
            // 1.XPENDING 查房：只读 idle 与投递次数，不改状态、不增加计数
            PendingMessages pendings = stringRedisTemplate.opsForStream().pending(
                    RedisConstants.SECKILL_STREAM_KEY, GROUP_NAME, Range.unbounded(), PENDING_BATCH_SIZE);
            for (PendingMessage p : pendings) {
                // 2.重试耗尽的毒消息：转死信止血。
                //   idle 极短时消费线程可能正在处理它，本轮跳过避免竞态，下一轮再收
                if (p.getTotalDeliveryCount() > MAX_DELIVERY_COUNT) {
                    if (p.getElapsedTimeSinceLastDelivery().compareTo(Duration.ofSeconds(2)) > 0) {
                        deadLetter(p);
                    }
                    continue;
                }
                // 3.idle 不足阈值：可能正在处理，不抢
                if (p.getElapsedTimeSinceLastDelivery().compareTo(CLAIM_MIN_IDLE) < 0) {
                    continue;
                }
                // 4.XCLAIM 过户给 c1，min-idle-time 双保险；
                //   过户后由主循环现有的 handlePendingList（XREADGROUP 0）读走处理
                stringRedisTemplate.execute((RedisCallback<List<ByteRecord>>) connection ->
                        connection.xClaim(RedisConstants.SECKILL_STREAM_KEY.getBytes(StandardCharsets.UTF_8),
                                GROUP_NAME, CONSUMER_NAME, CLAIM_MIN_IDLE, p.getId()));
                log.warn("已认领滞留订单消息，msgId={}，原消费者={}，idle={}ms，投递次数={}",
                        p.getIdAsString(), p.getConsumerName(),
                        p.getElapsedTimeSinceLastDelivery().toMillis(), p.getTotalDeliveryCount());
            }
        } catch (Exception e) {
            if (containsError(e, "NOGROUP")) {
                // 与主循环同款自愈：启动期建组失败时就地重建
                createConsumerGroupIfAbsent();
            } else {
                // 巡检失败只记日志，下个周期再来，绝不影响消费主循环
                log.error("pending-list 巡检异常", e);
            }
        }
    }

    /**
     * 重试耗尽的毒消息转死信。
     * 操作顺序是硬约束：先转死信留痕 → 再释放一人一单标记 → 最后 XACK；
     * 若颠倒（如先 ACK），中途崩溃会既丢消息又留下不一致状态，用户被永久锁死。
     */
    private void deadLetter(PendingMessage p) {
        // 1.XPENDING 只有元数据没有消息体，用 XRANGE 按 id 取回原始字段
        List<MapRecord<String, Object, Object>> records = stringRedisTemplate.opsForStream()
                .range(RedisConstants.SECKILL_STREAM_KEY, Range.closed(p.getId().getValue(), p.getId().getValue()));
        Map<Object, Object> value = (records == null || records.isEmpty()) ? null : records.get(0).getValue();
        VoucherOrder voucherOrder = null;
        if (value != null) {
            try {
                voucherOrder = BeanUtil.fillBeanWithMap(value, new VoucherOrder(), true);
            } catch (Exception e) {
                log.error("死信消息体解析失败，msgId=" + p.getIdAsString(), e);
            }
        }
        boolean identified = voucherOrder != null
                && voucherOrder.getUserId() != null && voucherOrder.getVoucherId() != null;

        // 2.区分毒消息成因：订单已落库说明是"XACK 反复失败"型，
        //   只补 ACK，严禁释放标记（否则有订单的用户资格被误放回）
        if (identified && query().eq("user_id", voucherOrder.getUserId())
                .eq("voucher_id", voucherOrder.getVoucherId()).count() > 0) {
            stringRedisTemplate.opsForStream()
                    .acknowledge(RedisConstants.SECKILL_STREAM_KEY, GROUP_NAME, p.getId());
            log.warn("订单已落库，仅补 XACK，msgId={}", p.getIdAsString());
            return;
        }

        // 3.先转死信留痕：原始字段 + 原消息 id + 死信原因，供人工排查
        Map<Object, Object> dead = value == null ? new HashMap<>() : new HashMap<>(value);
        dead.put("originMsgId", p.getIdAsString());
        dead.put("deadReason", "重试耗尽，投递次数=" + p.getTotalDeliveryCount());
        stringRedisTemplate.opsForStream().add(RedisConstants.SECKILL_DEAD_STREAM_KEY, dead);
        // 4.再释放一人一单标记，让用户后续还能重试；不回补 Redis 库存（宁可少卖不超卖）
        if (identified) {
            stringRedisTemplate.opsForSet().remove(
                    RedisConstants.SECKILL_ORDER_KEY + voucherOrder.getVoucherId(),
                    voucherOrder.getUserId().toString());
        }
        // 5.最后 XACK 移除 PEL
        stringRedisTemplate.opsForStream()
                .acknowledge(RedisConstants.SECKILL_STREAM_KEY, GROUP_NAME, p.getId());
        log.error("订单消息重试耗尽转死信，msgId={}，userId={}，voucherId={}，投递次数={}",
                p.getIdAsString(), identified ? voucherOrder.getUserId() : null,
                identified ? voucherOrder.getVoucherId() : null, p.getTotalDeliveryCount());
    }

    /**
     * 判断异常链中是否包含指定错误关键字。
     * Spring Data Redis 会把 Redis 错误包成 RedisSystemException，这里沿 cause 链逐层匹配。
     */
    private boolean containsError(Throwable e,String keyword){
        for(Throwable cause = e; cause != null; cause = cause.getCause()){
            if(cause.getMessage()!= null && cause.getMessage().contains(keyword)){
                return true;
            }
        }
        return false;
    }

    @Override
    public Result seckillVoucher(Long voucherId) {
        // 1.获取当前登录用户
        Long userId = UserHolder.getUser().getId();
        // 2.生成全局唯一订单 id
        long orderId = redisIdWorker.nextId("order");

        // 3.执行 Lua 脚本：活动时间、库存校验、一人一单、扣减库存与投递消息在 Redis 内原子完成
        Long result = stringRedisTemplate.execute(
                SECKILL_SCRIPT,
                Collections.emptyList(),
                voucherId.toString(),
                userId.toString(),
                String.valueOf(orderId),
                // 当前时间由 Java 传入而不是脚本内调用 TIME：TIME 属于写命令，在只读副本上会执行失败；
                // "一人一单"标记的 TTL 也一并传入，避免脚本里出现硬编码的魔法值
                String.valueOf(Instant.now().getEpochSecond()),
                String.valueOf(SECKILL_ORDER_TTL_SECONDS));

        if (result == null) {
            // 连接异常等情况下脚本可能返回空，按失败处理，避免把 500 抛给前端
            return Result.fail("秒杀失败，请稍后重试");
        }
        // 4.非 0 代表未获得购买资格，按脚本约定映射提示
        switch (result.intValue()) {
            case 0:
                return Result.ok(orderId);
            case 1:
                return Result.fail("库存不足");
            case 2:
                return Result.fail("不能重复下单");
            case 4:
                return Result.fail("秒杀尚未开始");
            case 5:
                return Result.fail("秒杀已结束");
            default:
                return Result.fail("秒杀失败，请稍后重试");
        }
    }

    @Transactional
    @Override
    public void createVoucherOrder(VoucherOrder voucherOrder) {
        Long userId = voucherOrder.getUserId();
        Long voucherId = voucherOrder.getVoucherId();

        // 1.数据库再查一次：数据库是最终事实源，这里同时承担消息重复投递时的幂等职责
        int count = query().eq("user_id", userId).eq("voucher_id", voucherId).count();
        if (count > 0) {
            // 订单确实存在，说明 Redis 里的"一人一单"标记是正确的，保留不动
            log.error("用户已经购买过了，userId=" + userId + "，voucherId=" + voucherId);
            return;
        }

        // 2.乐观锁扣减库存：判断与扣减在同一条 SQL 内完成，靠数据库行锁保证不会超卖
        boolean success = seckillVoucherService.update()
                .setSql("stock = stock - 1")
                .eq("voucher_id", voucherId)
                .gt("stock", 0)
                .update();
        if (!success) {
            // Redis 放行但数据库库存不足，说明两边已不一致（例如后台把库存改小了）。
            // 这里只撤销该用户的"一人一单"标记，让他后续还能重试；不回补 Redis 库存，
            // 因为库存是预扣配额，回补会让后续请求突破真实库存——宁可少卖，也不能超卖。
            stringRedisTemplate.opsForSet()
                    .remove(RedisConstants.SECKILL_ORDER_KEY + voucherId, userId.toString());
            log.error("库存不足，已回滚一人一单标记，userId=" + userId + "，voucherId=" + voucherId);
            return;
        }

        // 3.订单落库
        save(voucherOrder);
    }
}
