package io.falcondb.proc.trigger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Registry that maps (table, timing, operation) → ordered list of TriggerHandlers.
 * Multiple handlers on the same slot fire in registration order, matching PG trigger ordering.
 */
public class TriggerRegistry {

    private final Map<TriggerKey, List<TriggerHandler>> handlers = new ConcurrentHashMap<>();

    /**
     * Register a trigger handler.
     *
     * @param table     table name (case-insensitive)
     * @param timing    BEFORE or AFTER
     * @param ops       one or more operations this trigger fires on
     * @param handler   the handler implementation
     */
    public TriggerRegistry on(String table, TriggerTiming timing, TriggerOperation[] ops, TriggerHandler handler) {
        for (TriggerOperation op : ops) {
            TriggerKey key = new TriggerKey(table.toLowerCase(), timing, op);
            handlers.computeIfAbsent(key, k -> Collections.synchronizedList(new ArrayList<>())).add(handler);
        }
        return this;
    }

    public TriggerRegistry on(String table, TriggerTiming timing, TriggerOperation op, TriggerHandler handler) {
        return on(table, timing, new TriggerOperation[]{op}, handler);
    }

    List<TriggerHandler> get(String table, TriggerTiming timing, TriggerOperation op) {
        List<TriggerHandler> list = handlers.get(new TriggerKey(table.toLowerCase(), timing, op));
        return list != null ? list : Collections.emptyList();
    }

    boolean hasBefore(String table, TriggerOperation op) {
        return !get(table, TriggerTiming.BEFORE, op).isEmpty();
    }

    boolean hasAfter(String table, TriggerOperation op) {
        return !get(table, TriggerTiming.AFTER, op).isEmpty();
    }

    private static final class TriggerKey {
        final String table;
        final TriggerTiming timing;
        final TriggerOperation op;

        TriggerKey(String table, TriggerTiming timing, TriggerOperation op) {
            this.table = table;
            this.timing = timing;
            this.op = op;
        }

        @Override
        public boolean equals(Object o) {
            if (!(o instanceof TriggerKey)) return false;
            TriggerKey k = (TriggerKey) o;
            return table.equals(k.table) && timing == k.timing && op == k.op;
        }

        @Override
        public int hashCode() {
            return table.hashCode() * 31 * 31 + timing.hashCode() * 31 + op.hashCode();
        }
    }
}
