package com.msb.es.entity;


import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableId;
import com.baomidou.mybatisplus.annotation.TableName;
import lombok.Data;

import java.math.BigDecimal;

@Data
@TableName(value = "hy_seriesinfo")
public class HySeriesInfo {
    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;
    private String name;
    private BigDecimal minprice;
    private BigDecimal maxprice;
    private String fctName;
    private Integer fctId;
    private Integer brandId;
    private String brandName;
    private String brandLogo;
}