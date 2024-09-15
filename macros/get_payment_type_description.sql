{ #

    This macro returns the description of the payment type
#}

{% macro get_payment_type_description(payment_type) %}

    case {{ dbt.safe_cast("payment_type", api.Column.translate_type("numeric")) }} 
        when 1.0 then 'Credit Card'
        when 2.0 then 'Cash'
        when 3.0 then 'No charge'
        when 4.0 then 'Dispute'
        when 5.0 then 'Unknown'
        when 6.0 then 'Voided trip'
        else 'EMPTY'
    end

{% endmacro %}