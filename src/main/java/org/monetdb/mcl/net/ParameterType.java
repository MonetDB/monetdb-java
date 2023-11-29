package org.monetdb.mcl.net;

public enum ParameterType {
    Str,
    Int,
    Bool,
    Path;

    public Object parse(String name, String value) throws ValidationError {
        if (value == null)
            throw new NullPointerException();

        try {
            switch (this) {
                case Bool:
                    return parseBool(value);
                case Int:
                    return Integer.parseInt(value);
                case Str:
                case Path:
                    return value;
                default:
                    throw new IllegalStateException("unreachable");
            }
        } catch (IllegalArgumentException e) {
            String message = e.toString();
            throw new ValidationError(name, message);
        }
    }

    public String format(Object value) {
        switch (this) {
                case Bool:
                    return (Boolean)value ? "true": "false";
                case Int:
                    return Integer.toString((Integer)value);
                case Str:
                case Path:
                    return (String) value;
                default:
                    throw new IllegalStateException("unreachable");
            }
    }

    public static boolean parseBool(String value) {
        boolean lowered = false;
        String original = value;
        while (true) {
            switch (value) {
                case "true":
                case "yes":
                case "on":
                    return true;
                case "false":
                case "no":
                case "off":
                    return false;
                default:
                    if (!lowered) {
                        value = value.toLowerCase();
                        lowered = true;
                        continue;
                    }
                    throw new IllegalArgumentException("invalid boolean value: " + original);
            }
        }
    }
}
