package io.falcondb.proc.trigger;

import io.falcondb.proc.ProcException;

/**
 * Interface that each migrated trigger function must implement.
 * Name the class Trigger_<table>_<name> and register it in TriggerRegistry.
 */
public interface TriggerHandler {
    TriggerResult fire(TriggerEvent event) throws ProcException;
}
