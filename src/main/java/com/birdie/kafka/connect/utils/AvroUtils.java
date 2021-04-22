package com.birdie.kafka.connect.utils;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class AvroUtils {
    private static final String REPLACEMENT_CHAR = "_";
    private static final String NUMBER_PREFIX = "_";

    private static final Logger LOGGER = LoggerFactory.getLogger(AvroUtils.class);

    /**
     * Sanitize column names that are illegal in Avro
     * Must conform to https://avro.apache.org/docs/1.7.7/spec.html#Names
     *  Legal characters are [a-zA-Z_] for the first character and [a-zA-Z0-9_] thereafter.
     *
     * @param columnName the column name name to be sanitized
     *
     * @return the sanitized name.
     */
    static public String sanitizeColumnName(String columnName) {
        boolean changed = false;
        StringBuilder sanitizedNameBuilder = new StringBuilder(columnName.length() + 1);
        for (int i = 0; i < columnName.length(); i++) {
            char c = columnName.charAt(i);
            if (i == 0 && Character.isDigit(c)) {
                sanitizedNameBuilder.append(NUMBER_PREFIX);
                sanitizedNameBuilder.append(c);
                changed = true;
            }
            else if (!(c == '_' || (c >= 'A' && c <= 'Z') || (c >= 'a' && c <= 'z') || (c >= '0' && c <= '9'))) {
                sanitizedNameBuilder.append(REPLACEMENT_CHAR);
                changed = true;
            }
            else {
                sanitizedNameBuilder.append(c);
            }
        }

        final String sanitizedName = sanitizedNameBuilder.toString();
        if (changed) {
            LOGGER.warn("Field '{}' name potentially not safe for serialization, replaced with '{}'", columnName, sanitizedName);
        }

        return sanitizedName;
    }
}
