\set ON_ERROR_STOP on

-- 이 파일은 시작 차단용 read-only contract check다. PGOPTIONS가 쓰기를 금지한다.
WITH expected_public_relations(relation_name) AS (
    VALUES
        ('weather_grid'),
        ('station'),
        ('station_stock'),
        ('station_demand_forecast'),
        ('weather_forecast'),
        ('event'),
        ('station_urgency'),
        ('dispatch_center'),
        ('rebalance_route'),
        ('rebalance_route_stop')
),
public_relations AS (
    SELECT c.oid, c.relname, c.relkind
      FROM pg_class AS c
      JOIN pg_namespace AS n ON n.oid = c.relnamespace
     WHERE n.nspname = 'public'
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
       AND NOT EXISTS (
               SELECT 1
                 FROM pg_depend AS d
                 JOIN pg_extension AS e ON e.oid = d.refobjid
                WHERE d.classid = 'pg_class'::REGCLASS
                  AND d.objid = c.oid
                  AND d.deptype = 'e'
           )
),
gold_relations AS (
    SELECT c.oid, c.relname, c.relkind
      FROM pg_class AS c
      JOIN pg_namespace AS n ON n.oid = c.relnamespace
     WHERE n.nspname = 'gold_meta'
       AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
),
expected_functions(schema_name, function_name, argument_count) AS (
    VALUES
        ('public', 'gold_set_updated_dttm', 0),
        ('public', 'gold_initialize_metadata_dttm', 0),
        ('public', 'gold_initialize_created_dttm', 0),
        ('gold_meta', 'protect_publication_state', 0),
        ('gold_meta', 'claim_publication', 7),
        ('gold_meta', 'lock_topology_shared', 0),
        ('gold_meta', 'lock_topology_exclusive', 0),
        ('gold_meta', 'lock_route_operation', 0),
        ('public', 'gold_lock_topology_write', 0),
        ('public', 'gold_lock_route_write', 0),
        ('public', 'gold_validate_rebalance_route_mutation', 0),
        ('public', 'gold_validate_rebalance_route_insert', 0),
        ('public', 'gold_validate_station_center_assignment', 0),
        ('public', 'gold_validate_dispatch_center_deactivation', 0),
        ('public', 'gold_protect_rebalance_route_delete', 0),
        ('public', 'gold_protect_rebalance_route_stop_mutation', 0),
        ('public', 'gold_ensure_rebalance_route_has_stop', 0),
        ('public', 'gold_ensure_rebalance_route_keeps_stop', 0)
),
contract_functions AS (
    SELECT n.nspname AS schema_name, p.proname AS function_name, p.pronargs AS argument_count
      FROM pg_proc AS p
      JOIN pg_namespace AS n ON n.oid = p.pronamespace
     WHERE n.nspname IN ('public', 'gold_meta')
       AND p.prokind = 'f'
       AND NOT EXISTS (
               SELECT 1
                 FROM pg_depend AS d
                 JOIN pg_extension AS e ON e.oid = d.refobjid
                WHERE d.classid = 'pg_proc'::REGCLASS
                  AND d.objid = p.oid
                  AND d.deptype = 'e'
           )
),
metadata_tables(schema_name, relation_name) AS (
    VALUES
        ('gold_meta', 'publication_state'),
        ('public', 'weather_grid'),
        ('public', 'station'),
        ('public', 'station_stock'),
        ('public', 'station_demand_forecast'),
        ('public', 'weather_forecast'),
        ('public', 'event'),
        ('public', 'station_urgency'),
        ('public', 'dispatch_center'),
        ('public', 'rebalance_route')
),
expected_special_triggers(schema_name, relation_name, trigger_name) AS (
    VALUES
        ('gold_meta', 'publication_state', 'protect_publication_state'),
        ('public', 'rebalance_route_stop', 'initialize_created_dttm'),
        ('public', 'rebalance_route', 'validate_rebalance_route_insert'),
        ('public', 'weather_grid', 'lock_topology_write'),
        ('public', 'dispatch_center', 'lock_topology_write'),
        ('public', 'station', 'lock_topology_write'),
        ('public', 'rebalance_route', 'lock_route_write'),
        ('public', 'rebalance_route_stop', 'lock_route_write'),
        ('public', 'station', 'validate_station_center_assignment'),
        ('public', 'dispatch_center', 'validate_dispatch_center_deactivation'),
        ('public', 'rebalance_route', 'validate_rebalance_route_mutation'),
        ('public', 'rebalance_route', 'protect_rebalance_route_delete'),
        ('public', 'rebalance_route_stop', 'protect_rebalance_route_stop_mutation'),
        ('public', 'rebalance_route', 'rebalance_route_has_stop'),
        ('public', 'rebalance_route_stop', 'rebalance_route_keeps_stop')
),
contract_triggers AS (
    SELECT n.nspname AS schema_name,
           c.relname AS relation_name,
           t.tgname AS trigger_name,
           t.tgdeferrable,
           t.tginitdeferred
      FROM pg_trigger AS t
      JOIN pg_class AS c ON c.oid = t.tgrelid
      JOIN pg_namespace AS n ON n.oid = c.relnamespace
     WHERE n.nspname IN ('public', 'gold_meta')
       AND NOT t.tgisinternal
),
expected_gist_indexes(relation_name, index_name, index_expression) AS (
    VALUES
        ('station', 'station_point_geography_gix', 'sta_point::geography'),
        ('event', 'event_point_geography_gix', 'event_point::geography'),
        (
            'dispatch_center',
            'dispatch_center_point_geography_gix',
            'dispatch_center_point::geography'
        )
),
contract_gist_indexes AS (
    SELECT target.relname AS relation_name,
           index_relation.relname AS index_name,
           pg_get_expr(i.indexprs, i.indrelid, true) AS index_expression
      FROM pg_index AS i
      JOIN pg_class AS target ON target.oid = i.indrelid
      JOIN pg_namespace AS n ON n.oid = target.relnamespace
      JOIN pg_class AS index_relation ON index_relation.oid = i.indexrelid
      JOIN pg_am AS am ON am.oid = index_relation.relam
     WHERE n.nspname = 'public'
       AND am.amname = 'gist'
       AND i.indisvalid
       AND i.indisready
),
public_acl_on_gold_schema AS (
    SELECT 1
      FROM pg_namespace AS n
      CROSS JOIN LATERAL aclexplode(COALESCE(n.nspacl, acldefault('n', n.nspowner))) AS acl
     WHERE n.nspname = 'gold_meta'
       AND acl.grantee = 0
),
public_acl_on_publication_state AS (
    SELECT 1
      FROM pg_class AS c
      JOIN pg_namespace AS n ON n.oid = c.relnamespace
      CROSS JOIN LATERAL aclexplode(COALESCE(c.relacl, acldefault('r', c.relowner))) AS acl
     WHERE n.nspname = 'gold_meta'
       AND c.relname = 'publication_state'
       AND acl.grantee = 0
),
public_acl_on_protected_functions AS (
    SELECT 1
      FROM pg_proc AS p
      JOIN pg_namespace AS n ON n.oid = p.pronamespace
      CROSS JOIN LATERAL aclexplode(COALESCE(p.proacl, acldefault('f', p.proowner))) AS acl
     WHERE n.nspname = 'gold_meta'
       AND p.proname IN (
               'claim_publication',
               'lock_topology_shared',
               'lock_topology_exclusive',
               'lock_route_operation'
           )
       AND acl.grantee = 0
)
SELECT EXISTS (
           SELECT 1
             FROM pg_extension
            WHERE extname = 'postgis'
              AND split_part(extversion, '.', 1) = '3'
              AND split_part(extversion, '.', 2) = '4'
       )
   AND current_setting('default_transaction_read_only') = 'on'
   AND :'airflow_db' <> current_database()
   AND EXISTS (
           SELECT 1
             FROM pg_database
            WHERE datname = :'airflow_db'
              AND datallowconn
       )
   AND (SELECT count(*) = 10 FROM public_relations)
   AND NOT EXISTS (
           SELECT relation_name FROM expected_public_relations
           EXCEPT
           SELECT relname FROM public_relations WHERE relkind = 'r'
       )
   AND (SELECT count(*) = 1 FROM gold_relations)
   AND EXISTS (
           SELECT 1
             FROM gold_relations
            WHERE relname = 'publication_state'
              AND relkind = 'r'
       )
   AND (SELECT count(*) = 18 FROM contract_functions)
   AND NOT EXISTS (
           SELECT 1
             FROM expected_functions AS expected
            WHERE NOT EXISTS (
                      SELECT 1
                        FROM contract_functions AS actual
                       WHERE actual.schema_name = expected.schema_name
                         AND actual.function_name = expected.function_name
                         AND actual.argument_count = expected.argument_count
                  )
       )
   AND EXISTS (
           SELECT 1
             FROM pg_proc AS p
             JOIN pg_namespace AS n ON n.oid = p.pronamespace
            WHERE n.nspname = 'gold_meta'
              AND p.proname = 'claim_publication'
              AND p.pronargs = 7
              AND p.prosecdef
       )
   AND (SELECT count(*) = 35 FROM contract_triggers)
   AND NOT EXISTS (
           SELECT 1
             FROM metadata_tables AS target
            WHERE EXISTS (
                      SELECT 1
                        FROM unnest(ARRAY['initialize_metadata_dttm', 'set_updated_dttm']) AS required(trigger_name)
                       WHERE NOT EXISTS (
                                 SELECT 1
                                   FROM contract_triggers AS actual
                                  WHERE actual.schema_name = target.schema_name
                                    AND actual.relation_name = target.relation_name
                                    AND actual.trigger_name = required.trigger_name
                             )
                  )
       )
   AND NOT EXISTS (
           SELECT 1
             FROM expected_special_triggers AS expected
            WHERE NOT EXISTS (
                      SELECT 1
                        FROM contract_triggers AS actual
                       WHERE actual.schema_name = expected.schema_name
                         AND actual.relation_name = expected.relation_name
                         AND actual.trigger_name = expected.trigger_name
                  )
       )
   AND NOT EXISTS (
           SELECT 1
             FROM expected_special_triggers AS expected
            WHERE expected.trigger_name IN (
                      'validate_station_center_assignment',
                      'validate_dispatch_center_deactivation',
                      'rebalance_route_has_stop',
                      'rebalance_route_keeps_stop'
                  )
              AND NOT EXISTS (
                      SELECT 1
                        FROM contract_triggers AS actual
                       WHERE actual.schema_name = expected.schema_name
                         AND actual.relation_name = expected.relation_name
                         AND actual.trigger_name = expected.trigger_name
                         AND actual.tgdeferrable
                         AND actual.tginitdeferred
                  )
       )
   AND (SELECT count(*) = 3 FROM contract_gist_indexes)
   AND NOT EXISTS (
           SELECT 1
             FROM expected_gist_indexes AS expected
            WHERE NOT EXISTS (
                      SELECT 1
                        FROM contract_gist_indexes AS actual
                       WHERE actual.relation_name = expected.relation_name
                         AND actual.index_name = expected.index_name
                         AND actual.index_expression = expected.index_expression
                  )
       )
   AND NOT EXISTS (SELECT 1 FROM public_acl_on_gold_schema)
   AND NOT EXISTS (SELECT 1 FROM public_acl_on_publication_state)
   AND NOT EXISTS (SELECT 1 FROM public_acl_on_protected_functions);
