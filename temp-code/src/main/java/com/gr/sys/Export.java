package com.gr.sys;

import com.gr.common.utils.JacksonUtil;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang3.StringUtils;

import java.io.*;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

public class Export {


    public static void main(String[] args) throws Exception{
        File file = new File("D:/data/a.csv");
        InputStream input = new FileInputStream(file);

        final InputStreamReader inputStreamReader = new InputStreamReader(input, "utf-8");
        final BufferedReader reader = new BufferedReader(inputStreamReader);
        final List<String> list = new ArrayList<>();
        String line = reader.readLine();
        int a = 0;
        while (line != null) {
            line = reader.readLine();
            if (StringUtils.isNotEmpty(line)) {
                String[] json = line.split("\\001");
                Map rowMap = JacksonUtil.json2Object(json[1], Map.class);

                if (rowMap == null) {
                    continue;
                }
                StringBuffer buff = new StringBuffer();
                buff.append(rowMap.get("NAME")).append(",").append(rowMap.get("NO_CER"));


                list.add(buff.toString());
                a ++;
                System.out.println(a);
//                if (a > 3) {
//                    break;
//                }

            }
        }

        FileUtils.writeLines(new File("D:/data/out.csv"), list);

    }
}
