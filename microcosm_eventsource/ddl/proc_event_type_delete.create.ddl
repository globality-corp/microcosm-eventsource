/*
 * proc_event_type_delete(table_name, old_event_type, model_id_row_name)
 *
 * Should be only used by data migrations scripts.
 * Delete events with old_event_type and remove it from states,
 * Must pass model_id_row_name - the row name that links to the parent model
 * (Example: for company_event table, the model_id_row_name should be company_id)
 *
 * Example:
 * SELECT proc_event_type_delete('task_event', 'SCHEDULED', 'task_id');
 *
 * Details:
 * - Updates parent_id of relevant child events
 * - Updates the state column (Ensures that the state is sorted and has unique events)
 * - Does not promise valid events or transitions.
 * - Cannot be used if model.__unique_parent__ is set to False (missing table_names_parent_id_key constraint),
 */
CREATE OR REPLACE FUNCTION proc_event_type_delete(
    table_name regclass,
    old_event_type character varying(255),
    model_id_row_name character varying(255)) RETURNS void AS
$func$
BEGIN
    EXECUTE format(
        '
            CREATE TEMP TABLE events_to_remove_%%1$s_%%2$s AS (
                SELECT id
                FROM %%1$s
                WHERE event_type = ''%%2$s''
            );
            SELECT proc_events_delete(''%%1$s'', ''events_to_remove_%%1$s_%%2$s'', ''%%3$s'');

            UPDATE %%1$s
            SET state = array_remove(state, ''%%2$s'')
            WHERE ''%%2$s'' = ANY(state);
        ',
        table_name,
        old_event_type,
        model_id_row_name
    );
END
$func$  LANGUAGE plpgsql;
