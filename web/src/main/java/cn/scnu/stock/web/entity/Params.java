package cn.scnu.stock.web.entity;


import io.swagger.models.auth.In;
import lombok.Data;
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.springframework.beans.factory.annotation.Autowired;

@Data
@EqualsAndHashCode(callSuper = false)
@Accessors(chain = true)

public class Params {
    private Integer op = 1;
    private String uuid = "test";
    private Integer history = 100;
    private float patience = 1;
    private Integer happen = 10;
    private boolean trend_flag = false;
    private Integer min_days = 3;
    private float var_threshold = 0.0005f;
    private float change_threshold = 0.01f;
    private Integer min_windows = 3;
    private Integer period = 3;

    public Params() {
    }

//    public Params(Integer op, Integer happen, Integer history, float patience, boolean trend_flag) {
//        this.op = op;
//        this.happen = happen;
//        this.history = history;
//        this.patience = patience;
//        this.trend_flag = trend_flag;
//    }
//
//    public Params(Integer op,Integer history, Integer min_days, float var_threshold, float change_threshold, Integer period) {
//        this.op = op;
//        this.history = history;
//        this.min_days = min_days;
//        this.var_threshold = var_threshold;
//        this.change_threshold = change_threshold;
//        this.period = period;
//    }
//
////    @Autowired
//    public Params(Integer op,Integer history) {
//        this.op = op;
//        this.history = history;
//    }
//
//    public Params(Integer op,float var_threshold, Integer history, Integer min_days, Integer period) {
//        this.op = op;
//        this.var_threshold = var_threshold;
//        this.history = history;
//        this.min_days = min_days;
//        this.period = period;
//    }
}
