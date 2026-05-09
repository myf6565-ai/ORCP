package com.orcp.ingest.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.orcp.ingest.entity.OrderEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * {@code orcp_detail.t_order} 的 MyBatis-Plus Mapper。
 *
 * <p>刻意保持简洁：摄入路径已由去重层保证幂等性，
 * 因此使用默认的 {@code insert} 方法即可，无需 {@code insertOrUpdate}。
 */
@Mapper
public interface OrderMapper extends BaseMapper<OrderEntity> {
}
