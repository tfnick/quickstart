package examples.streaming.antifraud.model;

import lombok.Data;

import java.io.Serializable;
import java.util.Date;
import java.util.List;

//申请或者提现动作
@Data
public class ApplyMessage implements Serializable {
    String gid;
    String cid;
    String et;//event type 1 申请 2 提现
    String orderNum;
    String idCard;
    String imei;
    String idfa;
    String mobile;
    String ip;
    Double money;
    //扩展时间
    Integer x2;
    Integer x3;
    Date applyTime;
    List<Contacts> contacts;

}
