package io.falcondb.proc.trigger.example;

import io.falcondb.proc.ProcException;
import io.falcondb.proc.Sql;
import io.falcondb.proc.trigger.TriggerEvent;
import io.falcondb.proc.trigger.TriggerHandler;
import io.falcondb.proc.trigger.TriggerResult;

import java.time.LocalDateTime;

/**
 * Example: migrated from a PostgreSQL trigger.
 *
 * Original PL/pgSQL:
 *   CREATE TRIGGER orders_audit_trigger
 *   AFTER INSERT OR UPDATE OR DELETE ON orders
 *   FOR EACH ROW EXECUTE FUNCTION orders_audit_fn();
 *
 *   CREATE OR REPLACE FUNCTION orders_audit_fn() RETURNS trigger AS $$
 *   BEGIN
 *     INSERT INTO orders_audit(op, order_id, changed_at)
 *     VALUES (TG_OP, COALESCE(NEW.id, OLD.id), NOW());
 *     RETURN NEW;
 *   END;
 *   $$ LANGUAGE plpgsql;
 */
public class Trigger_orders_audit implements TriggerHandler {

    @Override
    public TriggerResult fire(TriggerEvent event) throws ProcException {
        String op = event.getOperation().name();
        Object orderId;
        switch (event.getOperation()) {
            case DELETE:
                orderId = event.getOldRow().get("id");
                break;
            default:
                orderId = event.getNewRow().get("id");
        }

        Sql.execute(event.getConnection(),
                "INSERT INTO orders_audit(op, order_id, changed_at) VALUES (?, ?, ?)",
                op, orderId, LocalDateTime.now());

        return TriggerResult.proceed();
    }
}
