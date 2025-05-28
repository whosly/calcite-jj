package com.whosly.avacita.server.query.mask;

import lombok.*;
import org.apache.calcite.avatica.ColumnMetaData;

import java.util.Collections;
import java.util.List;

@Getter
@ToString
@AllArgsConstructor
@NoArgsConstructor
@Builder
public class ResultSetMeta {
    private String schemaName;

    private String tableName;

    /**
     * 查询的投影
     */
    private List<String> projects;

    /**
     * 查询的投影的 MetaData
     */
    private List<ColumnMetaData> projectMetaData;

    public static ResultSetMeta empty() {
        return ResultSetMeta.builder()
                .projects(Collections.emptyList())
                .build();
    }
}
