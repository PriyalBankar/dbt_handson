{% macro cents_to_dollars(column_name, precision=2) %}
    round({{ column_name }} / 100.0, {{ precision }})
{% endmacro %}


{% macro safe_divide(numerator, denominator, default=0) %}
    case
        when {{ denominator }} = 0 or {{ denominator }} is null
        then {{ default }}
        else {{ numerator }}::float / {{ denominator }}
    end
{% endmacro %}


{% macro current_fiscal_quarter() %}
    /*
    Returns the current fiscal quarter label (e.g. FY2024-Q3).
    Assumes fiscal year starts in February (common for retail).
    */
    'FY' || extract(year from dateadd('month', -1, current_date()))::varchar
    || '-Q' || ceil((extract(month from dateadd('month', -1, current_date())) / 3.0))::varchar
{% endmacro %}
