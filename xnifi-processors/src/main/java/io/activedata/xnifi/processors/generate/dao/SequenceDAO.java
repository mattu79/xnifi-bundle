package io.activedata.xnifi.processors.generate.dao;

import io.activedata.xnifi.processors.generate.sequence.Sequence;

/**
 * Created by MattU on 2017/12/14.
 */
public interface SequenceDAO {
    /**
     * 新增或者更新已有的序列参数
     * @param sequence
     */
    void saveSequence(Sequence sequence);

    /**
     * 取得已有序列参数
     * @param seqCode
     */
    Sequence getSequence(String seqCode);
}
