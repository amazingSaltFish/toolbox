package com.amazingsaltfish.model.kudu.column;

import com.amazingsaltfish.model.Column;
import lombok.Getter;
import lombok.Setter;
import org.apache.kudu.ColumnSchema;

/**
 * @author: wmh
 * Create Time: 2021/6/27 11:31
 */
@Setter
@Getter
public class KuduColumn extends Column {
    ColumnSchema columnSchema;
}
