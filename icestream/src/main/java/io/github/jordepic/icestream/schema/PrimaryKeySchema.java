package io.github.jordepic.icestream.schema;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import org.apache.iceberg.Schema;
import org.apache.iceberg.types.Types.NestedField;

public record PrimaryKeySchema(List<NestedField> fields) {

    public static PrimaryKeySchema parse(Schema schema, String commaSeparatedPrimaryKeyNames) {
        List<NestedField> parsed = new ArrayList<>();
        for (String name : commaSeparatedPrimaryKeyNames.split(",")) {
            String trimmed = name.trim();
            if (trimmed.isEmpty()) {
                continue;
            }
            NestedField field = schema.findField(trimmed);
            if (field == null) {
                throw new IllegalStateException("Primary key column not in schema: " + trimmed);
            }
            parsed.add(field);
        }
        if (parsed.isEmpty()) {
            throw new IllegalStateException("icestream.primary-keys is empty");
        }
        return new PrimaryKeySchema(List.copyOf(parsed));
    }

    public Set<Integer> fieldIds() {
        return fields.stream().map(NestedField::fieldId).collect(Collectors.toUnmodifiableSet());
    }
}
