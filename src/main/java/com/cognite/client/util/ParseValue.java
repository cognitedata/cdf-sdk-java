package com.cognite.client.util;

import com.google.common.base.Preconditions;
import com.google.protobuf.Value;

/**
 * This class hosts methods for parsing {@code Value} objects to various target types (String, double, etc.).
 *
 * This can be helpful in particular when working with CDF.Raw and Json (parsed to {@code Struct}).
 */
public class ParseValue {

    /**
     * Tries to parse a {@code Value} to a {@code String} representation.
     *
     * @param rawValue
     * @return
     */
    public static String parseString(Value rawValue) {
        Preconditions.checkNotNull(rawValue, "rawValue cannot be null");
        switch (rawValue.getKindCase()) {
            case STRING_VALUE:
                return rawValue.getStringValue().trim();
            case NUMBER_VALUE:
                return String.valueOf(rawValue.getNumberValue());
            case BOOL_VALUE:
                return String.valueOf(rawValue.getBoolValue());
            case LIST_VALUE:
                return rawValue.getListValue().toString();
            case STRUCT_VALUE:
                return rawValue.getStructValue().toString();
            case NULL_VALUE:
                return "null";
            default: // KIND_NOT_SET
                return "";
        }
    }

    /**
     * Tries to parse a {@code Value} to a {@code Double}. If the Value has a numeric or string representation the parsing
     * will succeed as long as the {@code Value} is within the Double range.
     *
     * Will throw a {@code NumberFormatException} if parsing is unsuccessful.
     * @param rawValue
     * @return
     * @throws NumberFormatException
     */
    public static double parseDouble(Value rawValue) throws NumberFormatException {
        Preconditions.checkNotNull(rawValue, "rawValue cannot be null");
        double returnDouble;
        if (rawValue.hasNumberValue()) {
            returnDouble = rawValue.getNumberValue();
        } else if (rawValue.hasStringValue()) {
            returnDouble = Double.parseDouble(rawValue.getStringValue());
        } else {
            throw new NumberFormatException("Unable to parse to double. "
                    + "Identified value type: " + rawValue.getKindCase()
                    + " Property value: " + rawValue.toString());
        }
        return returnDouble;
    }

    /**
     * Tries to parse a {@code Value} to a {@code Boolean}. If the Value has a boolean, numeric or string representation
     * the parsing will succeed.
     *
     * A bool {@code Value} representation is parsed directly.
     * A String {@code Value} representation returns true if the string argument is not null and equal to, ignoring case, the
     * string "true".
     * A numeric {@code Value} representation returns true if the number equals "1".
     *
     * Will throw an {@code Exception} if parsing is unsuccessful.
     * @param rawValue
     * @return
     * @throws Exception
     */
    public static boolean parseBoolean(Value rawValue) throws Exception {
        Preconditions.checkNotNull(rawValue, "rawValue cannot be null");
        boolean returnBoolean;
        if (rawValue.hasBoolValue()) {
            returnBoolean = rawValue.getBoolValue();
        } else if (rawValue.hasNumberValue()) {
            returnBoolean = Double.compare(1d, rawValue.getNumberValue()) == 0;
        } else if (rawValue.hasStringValue()) {
            returnBoolean = rawValue.getStringValue().equalsIgnoreCase("true");
        } else {
            throw new Exception("Unable to parse to boolean. "
                    + "Identified value type: " + rawValue.getKindCase()
                    + " Property value: " + rawValue.toString());
        }
        return returnBoolean;
    }
}
