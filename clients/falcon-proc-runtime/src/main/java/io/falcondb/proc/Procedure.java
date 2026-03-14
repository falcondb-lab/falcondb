package io.falcondb.proc;

/**
 * Interface that each migrated stored procedure must implement.
 * Name the class Proc_<original_name> and register it in ProcedureRegistry.
 */
public interface Procedure {
    ProcResult execute(ProcContext ctx, ProcArgs args) throws ProcException;
}
