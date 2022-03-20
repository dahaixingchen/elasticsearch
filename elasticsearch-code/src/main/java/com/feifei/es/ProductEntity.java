package com.feifei.es;

import com.baomidou.mybatisplus.annotation.IdType;
import com.baomidou.mybatisplus.annotation.TableField;
import com.baomidou.mybatisplus.annotation.TableId;
import lombok.Data;

import java.sql.Timestamp;

/**
 * @Description:
 * @ClassName: ProductEntity
 * @Author chengfei
 * @DateTime 2021/4/11 14:29
 **/
@Data
public class ProductEntity {
    @TableId(value = "id", type = IdType.AUTO)
    private Integer id;
    @TableField(value = "`name`")
    private String name;
    @TableField(value = "`desc`")
    private String desc;
    private double price;
    private String tags;
    private Timestamp createTime;

    public ProductEntity(String name, String desc, double price, String tags) {
        this.name = name;
        this.desc = desc;
        this.price = price;
        this.tags = tags;
    }
}
