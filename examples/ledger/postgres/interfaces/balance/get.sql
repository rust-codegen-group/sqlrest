SELECT CAST(COALESCE(sum(amount_minor), 0) AS BIGINT) AS balance_minor FROM entries;
