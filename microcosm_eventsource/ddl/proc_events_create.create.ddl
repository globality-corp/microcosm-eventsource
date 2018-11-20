/*
 * proc_events_create(table_name, events_to_create_table_name, columns)
 *
 * Should be only used by data migrations scripts.
 * Creates events from table table_name - a valid microcosm-event-source event table.
 * Pass the events to create as row from table events_to_create_table_name.
 * Must pass model_id_row_name - the row name that links to the parent model
 * Must pass the listen of expected columns to be inserted into the target event table.
 * Note: Events to create must not be initial events.
 * (Example: for company_event table, the model_id_row_name should be company_id)
 *
 * Example:
 *    CREATE TEMP TABLE events_to_create AS (\n"
 *           SELECT\n"
 *              '{reassigned_event_id}'::uuid as id,
 *              extract(epoch from now()) as created_at,
 *              extract(epoch from now()) as updated_at,
 *              assignee,
 *              NULL::timestamp without time zone as deadline,
 *              task_id,
 *              'REASSIGNED' as event_type,
 *              id as parent_id,
 *              state,
 *              1 as version
 *           FROM task_event WHERE event_type='ASSIGNED'
 *        );
 * FROM task_event WHERE event_type='SCHEDULED');
 * SELECT proc_events_create('task_event', 'events_to_create', 'task_id');
 *
 * Details:
 * - Won't update the state
 * - Creates the relevant events
 * - Updates the parent_id of events affected by inserted rows
 * - Cannot be used if model.__unique_parent__ is set to False (missing table_names_parent_id_key constraint),
 *   If thats the case - use proc_events_delete_with_no_parent_id_constraint function instead.
 */
CREATE OR REPLACE FUNCTION proc_events_create(
    table_name regclass,
    events_to_create_table_name regclass,
    columns character varying(255)
) RETURNS void AS
$func$
BEGIN
    EXECUTE format(
        '
            ALTER TABLE %%1$s DROP CONSTRAINT %%1$s_parent_id_key;
            SELECT proc_events_create_with_no_parent_id_constraint(''%%1$s'', ''%%2$s'', ''%%3$s'');
            ALTER TABLE %%1$s ADD CONSTRAINT %%1$s_parent_id_key UNIQUE (parent_id);
        ',
        table_name,
        events_to_create_table_name,
        columns
    );
END
$func$  LANGUAGE plpgsql;


/*
 * proc_events_create_with_no_parent_id_constraint(table_name, events_to_create_table_name, columns)
 *
 * Should be only used by data migrations scripts.
 * Same as proc_events_delete function but for use if model.__unique_parent__ is set to False
 * (missing table_names_parent_id_key constraint),
 */
CREATE OR REPLACE FUNCTION proc_events_create_with_no_parent_id_constraint(
    table_name regclass,
    events_to_create_table_name regclass,
    columns character varying(255)
) RETURNS void AS
$func$
BEGIN
    EXECUTE format(
        '
            -- Temporary drop the constraint, bring it back before the end of the transaction.
             ALTER TABLE %%1$s DROP CONSTRAINT %%1$s_parent_id_fkey;

            -- Create new events
            INSERT INTO %%1$s %%3$s (SELECT * FROM %%2$s);

            -- Updates parent_id of events if by creating a new event, gives an existing event a new parent.
            WITH new_child_events_parents AS (
                SELECT child_event.id AS child_event_id, parent_event.id AS new_parent_id             
                FROM %%1$s AS child_event
                JOIN %%2$s AS parent_event 
                ON child_event.parent_id = parent_event.parent_id
                where parent_event.id != child_event.id                            
            )
            UPDATE %%1$s
            SET parent_id = new_child_events_parents.new_parent_id                                    
            FROM new_child_events_parents
            WHERE %%1$s.id = new_child_events_parents.child_event_id;  

            ALTER TABLE %%1$s ADD CONSTRAINT %%1$s_parent_id_fkey FOREIGN KEY (parent_id) REFERENCES %%1$s(id);
        ',
        table_name,
        events_to_create_table_name,
        columns
    );
END
$func$  LANGUAGE plpgsql;
