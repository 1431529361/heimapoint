-- 1.参数列表
-- 1.1.优惠券id
local voucherId = ARGV[1]
-- 1.2.用户id
local userId = ARGV[2]
-- 1.3.订单id
local orderId = ARGV[3]
-- 1.4.当前时间戳（秒），由 Java 传入：脚本内调用 TIME 属于写命令，只读副本会执行失败
local now = tonumber(ARGV[4])
-- 1.5."一人一单"标记 Set 的存活秒数，由 Java 传入避免脚本硬编码
local orderTtl = tonumber(ARGV[5])

-- 2.数据key
-- 2.1.库存key
local stockKey = 'seckill:stock:' .. voucherId
-- 2.2.订单key（已下单用户集合）
local orderKey = 'seckill:order:' .. voucherId
-- 2.3.活动开始/结束时间（秒级时间戳）
local beginKey = 'seckill:begin:' .. voucherId
local endKey = 'seckill:end:' .. voucherId

-- 3.脚本业务
-- 3.1.校验活动时间：key 不存在时跳过判断，兼容只预热了库存的存量券，
--     避免历史数据因为缺少起止时间而直接无法参与秒杀
local beginTime = tonumber(redis.call('get', beginKey))
if beginTime ~= nil and now < beginTime then
    -- 3.1.1.活动未开始，返回 4
    return 4
end
local endTime = tonumber(redis.call('get', endKey))
if endTime ~= nil and now > endTime then
    -- 3.1.2.活动已结束，返回 5
    return 5
end

-- 3.2.判断库存是否充足
--     先判 nil 再比较：库存 key 被清理或未预热时 tonumber 得到 nil，
--     直接 nil <= 0 会让整个脚本报错，这里统一按"无库存"返回
local stock = tonumber(redis.call('get', stockKey))
if stock == nil or stock <= 0 then
    -- 3.2.1.库存不足，返回 1
    return 1
end

-- 3.3.判断用户是否已下单 SISMEMBER orderKey userId
if redis.call('sismember', orderKey, userId) == 1 then
    -- 3.3.1.已存在，说明是重复下单，返回 2
    return 2
end

-- 3.4.扣库存 incrby stockKey -1
redis.call('incrby', stockKey, -1)
-- 3.5.记录下单用户 sadd orderKey userId
redis.call('sadd', orderKey, userId)
-- 3.6.顺延"一人一单"标记的存活时间：Set 默认永不过期会持续占用内存，
--     以最后一次下单时间为基准续期，活动结束且不再下单后自动回收
redis.call('expire', orderKey, orderTtl)
-- 3.7.发送消息到队列中 XADD stream.orders * k1 v1 k2 v2 ...
redis.call('xadd', 'stream.orders', '*', 'userId', userId, 'voucherId', voucherId, 'id', orderId)
return 0
