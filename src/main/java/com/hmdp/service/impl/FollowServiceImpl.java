package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import com.baomidou.mybatisplus.core.conditions.query.QueryWrapper;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Follow;
import com.hmdp.mapper.FollowMapper;
import com.hmdp.service.IFollowService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.service.IUserService;
import com.hmdp.utils.UserHolder;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;

import java.util.Collections;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class FollowServiceImpl extends ServiceImpl<FollowMapper, Follow> implements IFollowService {
    @Autowired
    private StringRedisTemplate stringRedisTemplate;
    @Autowired
    private IUserService userService;

    @Override
    public Result isFollow(Long followUserId) {
        //1.获取登录用户
        Long id = UserHolder.getUser().getId();
        //2.查询是否关注
        Integer count = query().eq("follow_user_id", followUserId).eq("user_id", id).count();
        return Result.ok(count>0);
    }

    @Override
    public Result follow(Long followUserId, Boolean isFollow) {
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        //判断是关注还是取关
        if(isFollow){
            //要关注,新增数据
            Follow follow = new Follow();
            follow.setUserId(userId);
            follow.setFollowUserId(followUserId);
            boolean isSuccess = save(follow);
            if(isSuccess){
                stringRedisTemplate.opsForSet().add(key,followUserId.toString());
            }
        }else {
            boolean isSuccess = remove(new QueryWrapper<Follow>().eq("follow_user_id", followUserId).eq("user_id", userId));
            if(isSuccess){
                stringRedisTemplate.opsForSet().remove(key,followUserId.toString());
            }

        }
        return Result.ok();
    }

    @Override
    public Result followCommon(Long id) {
        Long userId = UserHolder.getUser().getId();
        String key = "follows:" + userId;
        //求交集
        String key2 = "follows:" + id;
        Set<String> intersect = stringRedisTemplate.opsForSet().intersect(key, key2);
        if(intersect == null || intersect.isEmpty()){
            return Result.ok(Collections.emptyList());
        }
        List<Long> ids = intersect.stream().map(Long::valueOf).collect(Collectors.toList());
        //查询用户
        List<UserDTO> users = userService.listByIds(ids).stream().map(user -> BeanUtil.copyProperties(user, UserDTO.class)).collect(Collectors.toList());
        return Result.ok(users);
    }
}
