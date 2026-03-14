package io.falcondb.proc.trigger.example;

import io.falcondb.proc.ProcException;
import io.falcondb.proc.trigger.TriggerEvent;
import io.falcondb.proc.trigger.TriggerHandler;
import io.falcondb.proc.trigger.TriggerResult;

import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * Example: BEFORE UPDATE trigger that stamps updated_at automatically.
 *
 * Original PL/pgSQL:
 *   CREATE TRIGGER orders_set_updated_at
 *   BEFORE UPDATE ON orders
 *   FOR EACH ROW EXECUTE FUNCTION set_updated_at();
 *
 *   CREATE OR REPLACE FUNCTION set_updated_at() RETURNS trigger AS $$
 *   BEGIN
 *     NEW.updated_at := NOW();
 *     RETURN NEW;
 *   END;
 *   $$ LANGUAGE plpgsql;
 */
public class Trigger_orders_set_updated_at implements TriggerHandler {

    @Override
    public TriggerResult fire(TriggerEvent event) throws ProcException {
        Map<String, Object> row = new HashMap<>(event.getNewRow());
        row.put("updated_at", LocalDateTime.now());
        return TriggerResult.modify(row);
    }
}
