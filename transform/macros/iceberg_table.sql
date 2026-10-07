{#
  dbt-duckdb's `table` materialization builds `<model>__dbt_tmp` and renames it into place.
  DuckDB-Iceberg can't rename a table created in the same transaction, run CREATE OR REPLACE,
  or drop and recreate a table in one transaction. So this drops the table, commits, and
  recreates it: simple, but the table is briefly absent while the model builds.
#}
{% materialization iceberg_table, adapter='duckdb' %}

  {%- set existing_relation = load_cached_relation(this) -%}
  {%- set target_relation = this.incorporate(type='table') -%}

  {{ run_hooks(pre_hooks, inside_transaction=False) }}
  {{ run_hooks(pre_hooks, inside_transaction=True) }}

  {#- not drop_relation_if_exists(): it adds CASCADE, which DuckDB-Iceberg rejects -#}
  {% if existing_relation is not none %}
    {% call statement('drop_existing') -%}
      drop table if exists {{ existing_relation }}
    {%- endcall %}
    {{ adapter.commit() }}
  {% endif %}

  {% call statement('main') -%}
    {{ create_table_as(False, target_relation, compiled_code) }}
  {%- endcall %}

  {{ run_hooks(post_hooks, inside_transaction=True) }}

  {{ adapter.commit() }}

  {{ run_hooks(post_hooks, inside_transaction=False) }}

  {{ return({'relations': [target_relation]}) }}

{% endmaterialization %}
