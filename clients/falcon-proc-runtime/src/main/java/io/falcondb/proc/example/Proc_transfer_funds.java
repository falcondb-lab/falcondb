package io.falcondb.proc.example;

import io.falcondb.proc.*;

import java.util.Optional;

/**
 * Example: migrated from a PostgreSQL stored procedure.
 *
 * Original PL/pgSQL:
 *   CREATE PROCEDURE transfer_funds(from_id INT, to_id INT, amount NUMERIC)
 *   LANGUAGE plpgsql AS $$
 *   BEGIN
 *     UPDATE accounts SET balance = balance - amount WHERE id = from_id;
 *     UPDATE accounts SET balance = balance + amount WHERE id = to_id;
 *     IF (SELECT balance FROM accounts WHERE id = from_id) < 0 THEN
 *       RAISE EXCEPTION 'Insufficient funds';
 *     END IF;
 *   END;
 *   $$;
 */
public class Proc_transfer_funds implements Procedure {

    @Override
    public ProcResult execute(ProcContext ctx, ProcArgs args) throws ProcException {
        long fromId = ((Number) args.get(0)).longValue();
        long toId   = ((Number) args.get(1)).longValue();
        double amount = ((Number) args.get(2)).doubleValue();

        Sql.execute(ctx.getConnection(),
                "UPDATE accounts SET balance = balance - ? WHERE id = ?",
                amount, fromId);

        Sql.execute(ctx.getConnection(),
                "UPDATE accounts SET balance = balance + ? WHERE id = ?",
                amount, toId);

        Optional<Object> bal = Sql.queryScalar(ctx.getConnection(),
                "SELECT balance FROM accounts WHERE id = ?", fromId);

        if (bal.isPresent() && ((Number) bal.get()).doubleValue() < 0) {
            throw new ProcException("Insufficient funds", "P0001");
        }

        return ProcResult.void_(2);
    }
}
