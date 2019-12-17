package io.activedata.xnifi.expression;

import java.util.List;

public class Analyse {

    /**
     *从集合中获取合理的最大值（通过闸值过滤掉不合理值）
     * @param dataList
     * @param brake
     * @return
     */
    public static int maxByOutNoise(List<String> dataList, int brake){
        if(dataList.size()==0){
            return 0;
        }
        int sum = 0;
        int max = 0;
        for(int i=0;i<dataList.size();i++){
            try {
                int iValue = Integer.valueOf(dataList.get(i));
                sum+=iValue;
            }catch (Exception e){
                dataList.remove(i);
            }
        }
        int average = sum/dataList.size();
        for(int i=0;i<dataList.size();i++){
            int iValue = Integer.valueOf(dataList.get(i));
            int difference = iValue-average;
            if(difference*difference>brake){
                dataList.remove(i);
                continue;
            }
            if(max<iValue){
                max = iValue;
            }
        }
        return max;
    }
}
