package com.hmdp.service.impl;

import cn.hutool.core.bean.BeanUtil;
import cn.hutool.core.util.BooleanUtil;
import cn.hutool.core.util.StrUtil;
import cn.hutool.json.JSONObject;
import cn.hutool.json.JSONUtil;
import com.hmdp.dto.Result;
import com.hmdp.entity.Shop;
import com.hmdp.mapper.ShopMapper;
import com.hmdp.service.IShopService;
import com.baomidou.mybatisplus.extension.service.impl.ServiceImpl;
import com.hmdp.utils.CacheClient;
import com.hmdp.utils.RedisData;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.stereotype.Service;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;

import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

import static com.hmdp.utils.RedisConstants.*;

/**
 * <p>
 *  服务实现类
 * </p>
 *
 * @author 虎哥
 * @since 2021-12-22
 */
@Service
public class ShopServiceImpl extends ServiceImpl<ShopMapper, Shop> implements IShopService {
    @Resource
    private StringRedisTemplate stringRedisTemplate;
    @Resource
    private CacheClient cacheClient;

    @Override
    public Result queryById(Long id) {
        //缓存穿透
        //Shop shop = queryWithPassThrough(id);

        //互斥锁
        //Shop shop = queryWithMutex(id);


        Shop shop = cacheClient.queryWithMutex(CACHE_SHOP_KEY, id, Shop.class, id1 -> getById(id1), 30l, TimeUnit.MINUTES);

        return Result.ok(shop);
    }

    private static final ExecutorService CACHE_REBUILD_EXECUTOR = Executors.newFixedThreadPool(10);


    public Shop queryWithLogicalExpire(Long id){
        String cacheKey = CACHE_SHOP_KEY + id;
        //1.从redis查询店铺
        String shopJson = stringRedisTemplate.opsForValue().get(cacheKey);

        //2. 查到空直接返回
        if (StrUtil.isBlank(shopJson)){
            return null;
        }

        //3.命中获取对象判断是否逻辑过期
        RedisData redisData = JSONUtil.toBean(shopJson, RedisData.class);
        Shop shop = JSONUtil.toBean((JSONObject) redisData.getData(), Shop.class);
        LocalDateTime expireTime = redisData.getExpireTime();
        if (expireTime.isAfter(LocalDateTime.now())){
            return shop;
        }
        //4.已过期 缓存重健
        String lockKey = LOCK_SHOP_KEY + id;
        boolean isLock = tryLock(lockKey);
        if (isLock){
            CACHE_REBUILD_EXECUTOR.submit( ()->{
                try {
                    this.saveShopRedis(id,30L);
                } catch (Exception e) {
                    throw new RuntimeException(e);
                } finally {
                    unLock(lockKey);
                }

            });
        }



        return shop;
    }




    public void saveShopRedis(Long id ,Long expireSeconds){
        //1.查询店铺
        Shop shop = getById(id);
        //2.封装逻辑时间
        RedisData redisData = new RedisData();
        redisData.setData(shop);
        redisData.setExpireTime(LocalDateTime.now().plusSeconds(expireSeconds));

        //3.写入Redis
        stringRedisTemplate.opsForValue().set(CACHE_SHOP_KEY + id,JSONUtil.toJsonStr(redisData));
    }



    public Shop queryWithMutex(Long id){
        String cacheKey = CACHE_SHOP_KEY + id;
        //1.从redis查询店铺
        String shopJson = stringRedisTemplate.opsForValue().get(cacheKey);

        //2.1存在直接返回
        if (StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        //判断是否是空值  不是null 就是"" 我们设定的空值
        if (shopJson != null){//返回空值
            return null;
        }

        String lockKey = null;
        Shop shop = null;
        try {
            lockKey = "lock:shop:" + id;
            if (!tryLock(lockKey)){//没有获取到锁
                Thread.sleep(50);
                return  queryWithMutex(id);

            }

            //2.2 不存在数据库中查询
            shop = getById(id);
            //3.数据库都不存在
            if (shop == null){
                stringRedisTemplate.opsForValue().set(cacheKey,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
                return null;
            }
            //4.数据框存在就存在redis中

            stringRedisTemplate.opsForValue().set(cacheKey,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);
        } catch (InterruptedException e) {
            throw new RuntimeException(e);
        }finally {
            unLock(lockKey);
        }


        return shop;
    }

    public Shop queryWithPassThrough(Long id){
        String cacheKey = CACHE_SHOP_KEY + id;
        //1.从redis查询店铺
        String shopJson = stringRedisTemplate.opsForValue().get(cacheKey);

        //2.1存在直接返回
        if (StrUtil.isNotBlank(shopJson)){
            Shop shop = JSONUtil.toBean(shopJson, Shop.class);
            return shop;
        }

        //判断是否是空值  不是null 就是"" 我们设定的空值
        if (shopJson != null){//返回空值
            return null;
        }

        //2.2 不存在数据库中查询
        Shop shop = getById(id);
        //3.数据库都不存在
        if (shop == null){
            stringRedisTemplate.opsForValue().set(cacheKey,"",CACHE_NULL_TTL, TimeUnit.MINUTES);
            return null;
        }
        //4.数据框存在就存在redis中

        stringRedisTemplate.opsForValue().set(cacheKey,JSONUtil.toJsonStr(shop),CACHE_SHOP_TTL, TimeUnit.MINUTES);

        return shop;
    }


    private boolean tryLock(String key){
        Boolean flag = stringRedisTemplate.opsForValue().setIfAbsent(key, "1", 10, TimeUnit.SECONDS);
        return BooleanUtil.isTrue(flag);
    }

    private void unLock(String key){
        stringRedisTemplate.delete(key);
    }

    @Override
    public Result updateShop(Shop shop) {
        //先更新数据框，再删除缓存
        Long id = shop.getId();
        if (id == null) {
            return Result.fail("店铺id不为空");

        }
        //更新数据框
        updateById(shop);
        //删除缓存
        stringRedisTemplate.delete(CACHE_SHOP_KEY + id);
        return  Result.ok();

    }
}
