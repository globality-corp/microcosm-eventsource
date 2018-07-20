/*
 * proc_events_delete(table_name, events_to_delete_table_name, model_id_row_name)
 *
 * Should be only used by data migrations scripts.
 * Deletes events from table table_name - a valid microcosm-event-source event table.
 * Pass the events to delete as column id from table events_to_delete_table_name.
 * Must pass model_id_row_name - the row name that links to the parent model
 * (Example: for company_event table, the model_id_row_name should be company_id)
 *
 * Example:
 * CREATE TEMP TABLE events_to_remove AS (SELECT id FROM task_event WHERE event_type='SCHEDULED');
 * SELECT proc_events_delete('task_event', 'events_to_remove', 'task_id');
 *
 * Details:
 * - Won't update the state (see: proc_event_type_delete function)
 * - Deletes the relevant events
 * - Updates the parent_id of the child events
 * - Cannot be used if model.__unique_parent__ is set to False (missing table_names_parent_id_key constraint),
 *   If thats the case - use proc_events_delete_with_no_parent_id_constraint function instead.
 */
CREATE OR REPLACE FUNCTION proc_events_delete(
    table_name regclass,
    events_to_delete_table_name regclass,
    model_id_row_name character varying(255)) RETURNS void AS
$func$
BEGIN
    EXECUTE format(
        '
            ALTER TABLE %%1$s DROP CONSTRAINT %%1$s_parent_id_key;
            SELECT proc_events_delete_with_no_parent_id_constraint(''%%1$s'', ''%%2$s'', ''%%3$s'');
            ALTER TABLE %%1$s ADD CONSTRAINT %%1$s_parent_id_key UNIQUE (parent_id);
        ',
        table_name,
        events_to_delete_table_name,
        model_id_row_name
    );
END
$func$  LANGUAGE plpgsql;


/*
 * proc_events_delete_with_no_parent_id_constraint(table_name, events_to_delete_table_name, model_id_row_name)
 *
 * Should be only used by data migrations scripts.
 * Same as proc_events_delete function but for use if model.__unique_parent__ is set to False
 * (missing table_names_parent_id_key constraint),
 */
CREATE OR REPLACE FUNCTION proc_events_delete_with_no_parent_id_constraint(
    table_name regclass,
    events_to_delete_table_name regclass,
    model_id_row_name character varying(255)) RETURNS void AS
$func$
BEGIN
    EXECUTE format(
        '
            -- Temporary drop the constraint, bring it back before the end of the transaction.
            ALTER TABLE %%1$s DROP CONSTRAINT %%1$s_parent_id_fkey;

            -- Updates parent_id of events if the current parent_id is going to be deleted.
            -- Handle the case that the new parent_id is not the parent_id of the current parent. 
            -- (For example from the events chain a->b->c->d, both events b and c are going to be deleted) 
            -- Skip events that are going to be deleted anyway.
            -- Skip the new"top events" - events that are going to have no parent id.
            WITH child_events AS (
                SELECT child_event.id, child_event.clock, child_event.%%3$s
                FROM %%1$s AS child_event
                JOIN %%2$s AS parent_event
                ON parent_event.id = child_event.parent_id
                LEFT JOIN %%2$s AS events_to_avoid
                ON events_to_avoid.id = child_event.id
                WHERE events_to_avoid.id is null
            ),
            new_child_events_parents AS (
                SELECT distinct on (child_events.clock)
                    child_events.id AS child_event_id,
                    new_parent.id AS new_parent_id
                FROM %%1$s AS new_parent
                RIGHT JOIN child_events
                ON new_parent.%%3$s = child_events.%%3$s
                AND new_parent.clock < child_events.clock
                LEFT JOIN %%2$s AS events_to_avoid
                ON events_to_avoid.id = new_parent.id
                WHERE events_to_avoid.id is null
                ORDER BY child_events.clock, new_parent.clock desc
            )
            UPDATE %%1$s
            SET parent_id = new_child_events_parents.new_parent_id
            FROM new_child_events_parents
            WHERE %%1$s.id = new_child_events_parents.child_event_id;

            -- Delete the events
            DELETE FROM %%1$s
            USING %%2$s
            WHERE %%1$s.id = %%2$s.id;

            -- Set null parent id to the new "top events".
            UPDATE %%1$s
            SET parent_id = null
            FROM %%2$s
            WHERE %%1$s.parent_id = %%2$s.id;

            ALTER TABLE %%1$s ADD CONSTRAINT %%1$s_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES %%1$s(id);
        ',
        table_name,
        events_to_delete_table_name,
        model_id_row_name
    );
END
$func$  LANGUAGE plpgsql;
