package examples.streaming.antifraud.model;

import lombok.Data;

@Data
public class ModelAttr {
    //Request Profile
    String gid;
    Double money;


    //User Profile
    String id_card;
    Integer x2;
    Integer x3;

    //Phone Profile
    String mobile;


    //Imei Profile
    String imei;

    //Ip Profile
    String profile;


}
