CREATE TABLE target_countries AS
SELECT id, json_array_elements_text(properties->'country') AS target_country
FROM entities_datasets
;

CREATE TABLE sanctioned_by AS
SELECT id, json_array_elements_text(datasets_names) AS dataset_name
FROM entities_datasets;

CREATE INDEX ON entities_datasets (schema);
CREATE INDEX ON target_countries (id);
CREATE INDEX ON sanctioned_by (id);
CREATE INDEX ON datasets (dataset_name);


;
CREATE TABLE entities_datasets_small AS
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Address' ORDER BY random() LIMIT 1000) a0 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Airplane' ORDER BY random() LIMIT 1000) a1 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Associate' ORDER BY random() LIMIT 1000) a2 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'BankAccount' ORDER BY random() LIMIT 1000) a3 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Company' ORDER BY random() LIMIT 1000) a4 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'CryptoWallet' ORDER BY random() LIMIT 1000) a5 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Directorship' ORDER BY random() LIMIT 1000) a6 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Employment' ORDER BY random() LIMIT 1000) a7 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Family' ORDER BY random() LIMIT 1000) a8 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Identification' ORDER BY random() LIMIT 1000) a9 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'LegalEntity' ORDER BY random() LIMIT 1000) a10 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Membership' ORDER BY random() LIMIT 1000) a11 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Occupancy' ORDER BY random() LIMIT 1000) a12 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Organization' ORDER BY random() LIMIT 1000) a13 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Ownership' ORDER BY random() LIMIT 1000) a14 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Passport' ORDER BY random() LIMIT 1000) a15 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Person' ORDER BY random() LIMIT 1000) a16 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Position' ORDER BY random() LIMIT 1000) a17 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Representation' ORDER BY random() LIMIT 1000) a18 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Sanction' ORDER BY random() LIMIT 1000) a19 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Security' ORDER BY random() LIMIT 1000) a20 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'UnknownLink' ORDER BY random() LIMIT 1000) a21 UNION ALL
SELECT * FROM (SELECT * FROM entities_datasets WHERE schema = 'Vessel' ORDER BY random() LIMIT 1000) a22
;


SELECT * FROM (
    SELECT id, caption, schema, first_seen, last_seen, last_change, target,
            properties->'name'->>0 AS target_name,
           jsonb_agg(target_country) AS target_country,
           jsonb_agg(title) AS sanctioned_dataset,
           jsonb_agg(CASE WHEN publisher->>'country' IS NULL THEN '' ELSE publisher->>'country' END) AS sanctioned_by_country,
           jsonb_agg(publisher->>'country_label') AS sanctioned_by_country_label
    FROM entities_datasets_small e
    LEFT JOIN target_countries t USING (id)
    LEFT JOIN sanctioned_by s USING (id)
    LEFT JOIN datasets d USING(dataset_name)
    GROUP BY 1,2,3,4,5,6,7,8
) s
WHERE (target_country) ? 'ru' AND jsonb_array_length(sanctioned_by_country-'ru'-'') > 0

/*
 * Companies from Russia that are sanctioned by at least one other country than russia:
 * * WHERE (target_country) ? 'ru' AND schema = 'Company' AND jsonb_array_length(sanctioned_by_country-'ru'-'') > 0
 * * 33s -> 3982 rows
 * ALL from Russia that are sanctioned by at least one other country than russia:
 * * WHERE (target_country) ? 'ru' AND jsonb_array_length(sanctioned_by_country-'ru'-'') > 0
 * * 1m 41s -> 17014 rows
 */









