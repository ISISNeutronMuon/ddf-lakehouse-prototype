{% macro create_temp_view_from_parquet(view_name, path, recursive_lookup="true") %}
  CREATE TEMPORARY VIEW {{ view_name }}
  USING parquet
  OPTIONS (
    recursiveFileLookup "{{ recursive_lookup }}",
    path "{{ path }}",
    pathGlobFilter "*.parquet"
  );
{% endmacro %}
