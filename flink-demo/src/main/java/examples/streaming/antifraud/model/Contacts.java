package examples.streaming.antifraud.model;

import java.io.Serializable;

//联系人
public class Contacts implements Serializable {
    //1 父母 2 兄妹 3 朋友 -1 未知
    String rel;
    String mobile;

    public String getRel() {
        return rel;
    }

    public void setRel(String rel) {
        this.rel = rel;
    }

    public String getMobile() {
        return mobile;
    }

    public void setMobile(String mobile) {
        this.mobile = mobile;
    }
}
