package com.gotocompany.firehose.serializer.constant;

public enum TypecastTarget {
    INTEGER {
        @Override
        public Object cast(String input) {
            try {
                return Integer.valueOf(input);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid input for INTEGER: " + input, e);
            }
        }
    }, LONG {
        @Override
        public Object cast(String input) {
            try {
                return Long.valueOf(input);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid input for LONG: " + input, e);
            }
        }
    }, DOUBLE {
        @Override
        public Object cast(String input) {
            try {
                return Double.valueOf(input);
            } catch (NumberFormatException e) {
                throw new IllegalArgumentException("Invalid input for DOUBLE: " + input, e);
            }
        }
    }, STRING {
        @Override
        public Object cast(String input) {
            return String.valueOf(input);
        }
    };

    public abstract Object cast(String input);
}
