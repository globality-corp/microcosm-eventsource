/*
 * proc_event_type_replace(table_name, old_event_type, new_event_type)
 *
 * Should be only used by data migrations scripts.
 * Replaces old_event_type with new_event_type in table_name table,
 *
 * Example:
 * SELECT proc_event_type_replace('task_event', 'SCHEDULED', 'CANCELED');
 *
 * Details:
 * - Updates the event_type column
 * - Updates the state column (Ensures that the state is sorted and has unique events)
 * - Does not promise valid events or transitions.
 */

CREATE OR REPLACE FUNCTION proc_event_type_replace(
    table_name regclass,
    old_event_type character varying(255),
    new_event_type character varying(255)) RETURNS void AS
$func$
BEGIN
    EXECUTE format(
        '
            UPDATE %%1$s
            SET event_type = ''%%3$s''
            WHERE event_type = ''%%2$s'';

            UPDATE %%1$s
            SET state = array_sort_unique(array_replace(state, ''%%2$s'', ''%%3$s''))
            WHERE ''%%2$s'' = ANY(state);
        ',
        table_name,
        old_event_type,
        new_event_type
    );
END
$func$  LANGUAGE plpgsql;
