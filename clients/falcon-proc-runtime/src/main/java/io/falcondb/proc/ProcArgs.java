package io.falcondb.proc;

import java.util.ArrayList;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

public class ProcArgs {
    private final List<Object> positional;
    private final Map<String, Object> named;

    private ProcArgs(List<Object> positional, Map<String, Object> named) {
        this.positional = positional;
        this.named = named;
    }

    public static ProcArgs of(Object... args) {
        List<Object> list = new ArrayList<>();
        for (Object a : args) list.add(a);
        return new ProcArgs(list, new LinkedHashMap<>());
    }

    public static Builder builder() {
        return new Builder();
    }

    public Object get(int index) {
        return positional.get(index);
    }

    public Object get(String name) {
        return named.get(name);
    }

    public int size() {
        return positional.isEmpty() ? named.size() : positional.size();
    }

    /** Returns positional args as array for PreparedStatement.setObject usage. */
    public Object[] toArray() {
        return positional.toArray();
    }

    public static class Builder {
        private final List<Object> positional = new ArrayList<>();
        private final Map<String, Object> named = new LinkedHashMap<>();

        public Builder add(Object value) {
            positional.add(value);
            return this;
        }

        public Builder add(String name, Object value) {
            named.put(name, value);
            return this;
        }

        public ProcArgs build() {
            return new ProcArgs(positional, named);
        }
    }
}
