package com.orcp.ingest.mapper;

import com.baomidou.mybatisplus.core.mapper.BaseMapper;
import com.orcp.ingest.entity.OrderEntity;
import org.apache.ibatis.annotations.Mapper;

/**
 * MyBatis-Plus mapper for {@code orcp_detail.t_order}.
 *
 * <p>Kept intentionally minimal -- the default {@code insert} method is
 * enough; we rely on the dedup layer to guarantee the row doesn't already
 * exist, so we don't need {@code insertOrUpdate} in the ingest path.
 */
@Mapper
public interface OrderMapper extends BaseMapper<OrderEntity> {
}
