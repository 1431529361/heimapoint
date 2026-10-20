package com.hmdp.controller;


import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.RandomUtil;
import com.baomidou.mybatisplus.extension.plugins.pagination.Page;
import com.hmdp.dto.LoginFormDTO;
import com.hmdp.dto.Result;
import com.hmdp.dto.UserDTO;
import com.hmdp.entity.Blog;
import com.hmdp.entity.User;
import com.hmdp.entity.UserInfo;
import com.hmdp.service.IBlogService;
import com.hmdp.service.IUserInfoService;
import com.hmdp.service.IUserService;
import com.hmdp.utils.RedisConstants;
import com.hmdp.utils.RegexUtils;
import com.hmdp.utils.SystemConstants;
import com.hmdp.utils.UserHolder;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.web.bind.annotation.*;

import javax.annotation.Resource;
import javax.servlet.http.HttpServletRequest;
import javax.servlet.http.HttpSession;
import java.util.List;
import java.util.concurrent.TimeUnit;

/**
 * <p>
 * 前端控制器
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Slf4j
@RestController
@RequestMapping("/user")
public class UserController {

    @Resource
    private IUserService userService;

    @Resource
    private IUserInfoService userInfoService;
    @Autowired
    private StringRedisTemplate stringRedisTemplate;

    /**
     * 发送手机验证码
     */
    @PostMapping("code")
    public Result sendCode(@RequestParam("phone") String phone) {

        if(RegexUtils.isPhoneInvalid(phone)){
            return Result.fail("手机号格式错误");
        }
        String code = RandomUtil.randomNumbers(6);
        stringRedisTemplate.opsForValue().set(RedisConstants.LOGIN_CODE_KEY + phone,code, RedisConstants.LOGIN_CODE_TTL, TimeUnit.MINUTES);
        log.debug("发送验证码成功，验证码：{}",code);
        return Result.ok();
    }

    /**
     * 登录功能
     * @param loginForm 登录参数，包含手机号、验证码；或者手机号、密码
     */
    @PostMapping("/login")
    public Result login(@RequestBody LoginFormDTO loginForm){

        return userService.login(loginForm);

    }

    /**
     * 登出功能
     * @return 无
     */
    @PostMapping("/logout")
    public Result logout(HttpServletRequest request){
        String token = request.getHeader("authorization");
        if(token == null || token.isEmpty()){
            return Result.ok();

        }
        String key = RedisConstants.LOGIN_USER_KEY + token;
        stringRedisTemplate.delete(key);
        return Result.ok();
    }

    @GetMapping("/me")
    public Result me(){
        UserDTO userDTO = UserHolder.getUser();
        return Result.ok(userDTO);
    }

    @GetMapping("/info/{id}")
    public Result info(@PathVariable("id") Long userId){
        // 查询详情
        UserInfo info = userInfoService.getById(userId);
        if (info == null) {
            // 没有详情，应该是第一次查看详情
            return Result.ok();
        }
        info.setCreateTime(null);
        info.setUpdateTime(null);
        // 返回
        return Result.ok(info);
    }
    @GetMapping("/{id}")
    public Result queryUserById(@PathVariable("id") Long userId){
        //查询详情
        User user = userService.getById(userId);
        if (user == null) {
            return Result.ok();
        }
        UserDTO userDTO = BeanUtil.copyProperties(user, UserDTO.class);
        return Result.ok(userDTO);
    }
    @PostMapping("/sign")
    public Result sign(){
        return userService.sign();
    }
    @GetMapping("/sign/count")
    public Result signCount(){
        return userService.signCount();
    }


}
