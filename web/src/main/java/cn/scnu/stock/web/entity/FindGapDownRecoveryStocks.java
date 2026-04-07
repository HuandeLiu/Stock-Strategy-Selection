package cn.scnu.stock.web.entity;

import com.baomidou.mybatisplus.extension.activerecord.Model;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.springframework.stereotype.Component;

/**
 * (FindGapDownRecoveryStocks)表实体类
 *
 * @author makejava
 * @since 2025-03-07 13:51:57
 */
@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)
@Component
@SuppressWarnings("serial")
public class FindGapDownRecoveryStocks extends Model<FindGapDownRecoveryStocks> {
    private static final long serialVersionUID = 1L;

    private String id;

    private Integer history;

    private String ts_code;

    private String start_date;

    private String end_date;



}

