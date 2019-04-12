package com.gr.sys;

import com.gr.common.result.CodeConsts;
import com.gr.common.result.IResult;
import com.gr.common.utils.JacksonUtil;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import javax.servlet.http.HttpServletRequest;
import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;
import java.util.Map;

@RestController
@RequestMapping("x")
public class SyncController {

    @RequestMapping("/fraud")
    public IResult<Map> antiFraud(HttpServletRequest request) {
        String body = null;

        try {
            BufferedReader reader = new BufferedReader(new InputStreamReader(request.getInputStream()));
            body = IOUtils.toString(reader);
        } catch (IOException e) {
            e.printStackTrace();
        }

        if (StringUtils.isEmpty(body)) {
            return IResult.bizError(CodeConsts.BIZ_ERROR_CODE_PARAM_ERROR, "json参数为空");
        } else {
            Map params = JacksonUtil.jsonToObject(body, Map.class);

            if (params == null) {
                return IResult.bizError(CodeConsts.BIZ_ERROR_CODE_PARAM_ERROR, "json参数错误");
            }

            String gid = request.getParameter("gid");

            //发送kafka消息进行流式处理


            List<String> kv = RedisUtil.blpop(30, gid);

            RedisUtil.del(gid);

            if (kv==null||kv.isEmpty()) {
                return IResult.bizError(CodeConsts.SERVER_BIZ_ERROR_CODE_COMMON_ERROR, "30秒内处理未完成");
            } else {
                return IResult.success(JacksonUtil.json2Object(kv.get(1),Map.class));
            }


        }
    }
}
