package cn.scnu.stock.web.entity;

import com.baomidou.mybatisplus.annotation.TableName;
import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;

/**
 * (AverageAnalysis)表实体类
 *
 * @author makejava
 * @since 2025-03-02 13:19:49
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@TableName("average_analysis")
//@ApiModel(value="AverageAnalysis", description="")
//@SuppressWarnings("serial")
public class AverageAnalysis extends Model<AverageAnalysis> {
    private static final long serialVersionUID = 1L;

//    @TableId(value = "id",type = IdType.ASSIGN_UUID)
//    @ApiModelProperty(value = "ID", example = "1")
    private String id;

//    @ApiModelProperty(value = "操作日期", example = "")
    private String select_date;

    private Integer history;

    private Integer happen;

    private Float patience;

    private Boolean trend_flag;

    private String ts_code;

    private String ma5date;

    private String ma10date;

    private String ma20date;

    private String ma30date;

    private String ma60date;

}

