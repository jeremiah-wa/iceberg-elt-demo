{#
  Overrides of two dbt-duckdb 1.11.0 macros so the built-in `table` materialization works on
  Iceberg REST catalogs. Ported from duckdb/dbt-duckdb#747, which isn't merged. dbt dispatches
  both macros and looks in the root project first, so these replace dbt-duckdb's everywhere.
  Delete this file once a dbt-duckdb release includes the fix.
#}

{#
  True when the relation's database is an attachment of type iceberg in the current target.
  Stands in for #747's Python `adapter.is_iceberg()`, and also finds the type when it's set
  under `options`, as profiles.yml does.
#}
{% macro is_iceberg_relation(relation) %}
  {%- for attachment in target.get('attach') or [] -%}
    {%- set options = attachment.get('options') or {} -%}
    {%- set attach_type = attachment.get('type') or options.get('type') or '' -%}
    {%- set database = attachment.get('alias') or attachment.get('path') -%}
    {%- if attach_type | lower == 'iceberg' and (relation.database or '') | lower == database | lower -%}
      {{ return(true) }}
    {%- endif -%}
  {%- endfor -%}
  {{ return(false) }}
{% endmacro %}

{# DuckDB rejects DROP ... CASCADE on Iceberg tables, so leave it off, as dbt-duckdb does for DuckLake #}
{% macro duckdb__drop_relation(relation) -%}
  {% call statement('drop_relation', auto_begin=False) -%}
    {% if adapter.is_ducklake(relation) or is_iceberg_relation(relation) %}
      drop {{ relation.type }} if exists {{ relation }}
    {% else %}
      drop {{ relation.type }} if exists {{ relation }} cascade
    {% endif %}
  {%- endcall %}
{% endmacro %}

{#
  DuckDB can't rename an Iceberg table in the transaction that created or changed it, so
  commit first. The `table` materialization always has a transaction open here: the
  CREATE TABLE ... AS of `<model>__dbt_tmp`, or the previous rename.
#}
{% macro duckdb__rename_relation(from_relation, to_relation) -%}
  {% set target_name = adapter.quote_as_configured(to_relation.identifier, 'identifier') %}
  {% if is_iceberg_relation(from_relation) %}
    {% do adapter.commit() %}
  {% endif %}
  {% call statement('rename_relation') -%}
    alter {{ to_relation.type }} {{ from_relation }} rename to {{ target_name }}
  {%- endcall %}
{% endmacro %}
