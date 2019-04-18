package examples.streaming.antifraud.model;

public class HttpBlacklistResponse {

    //1 白名单 2 黑名单 -1 未知
    Integer ipLevel;

    //1 白名单 2 黑名单 -1 未知
    Integer idCardLevel;

    //1 白名单 2 黑名单 -1 未知
    Integer mobileLevel;

    public Integer getIpLevel() {
        return ipLevel;
    }

    public void setIpLevel(Integer ipLevel) {
        this.ipLevel = ipLevel;
    }

    public Integer getIdCardLevel() {
        return idCardLevel;
    }

    public void setIdCardLevel(Integer idCardLevel) {
        this.idCardLevel = idCardLevel;
    }

    public Integer getMobileLevel() {
        return mobileLevel;
    }

    public void setMobileLevel(Integer mobileLevel) {
        this.mobileLevel = mobileLevel;
    }
}
