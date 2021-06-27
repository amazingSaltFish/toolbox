package com.amazingsaltfish.model.kudu.table;

import lombok.Getter;
import lombok.Setter;

/**
 * @author: wmh
 * Create Time: 2021/6/27 12:10
 */

public class KuduWriteOptionBuilder {

    private Boolean ignoreDuplicateRowErrors;
    private Boolean ignoreNull;
    private Boolean repartition;
    private Boolean repartitionSort;
    private Boolean handleSchemaDrift;

    private KuduWriteOptionBuilder() {
        this.ignoreDuplicateRowErrors = false;
        this.ignoreNull = false;
        this.repartition = false;
        this.repartitionSort = false;
        this.handleSchemaDrift = false;
    }

    public static KuduWriteOptionBuilder builder() {
        return new KuduWriteOptionBuilder();
    }

    public KuduWriteOptionBuilder setIgnoreDuplicateRowErrors(Boolean ignoreDuplicateRowErrors) {
        this.ignoreDuplicateRowErrors = ignoreDuplicateRowErrors;
        return this;
    }


    public KuduWriteOptionBuilder setIgnoreNull(Boolean ignoreNull) {
        this.ignoreNull = ignoreNull;
        return this;
    }


    public KuduWriteOptionBuilder setRepartition(Boolean repartition) {
        this.repartition = repartition;
        return this;
    }


    public KuduWriteOptionBuilder setRepartitionSort(Boolean repartitionSort) {
        this.repartitionSort = repartitionSort;
        return this;
    }

    public KuduWriteOptionBuilder setHandleSchemaDrift(Boolean handleSchemaDrift) {
        this.handleSchemaDrift = handleSchemaDrift;
        return this;
    }
}
