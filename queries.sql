SELECT * FROM entities_datasets;

SELECT
    id, caption,
    CASE
        WHEN schema = 'Address' OR schema = 'Airplane' OR schema = 'BankAccount' OR schema = 'CryptoWallet' THEN properties->'country'
        WHEN schema='Company' THEN COALESCE(properties->'mainCountry', properties->'country')
    END AS countries,

    CASE
        WHEN schema = 'Address' THEN properties->'full'
        WHEN schema = 'Airplane' OR schema='Company' OR schema = 'CryptoWallet' THEN properties->'address'
    END AS address_full,

    CASE
        WHEN schema = 'Airplane' THEN COALESCE(properties->'owner', properties->'operator')
        WHEN schema = 'Associate' THEN properties->'person'
        WHEN schema = 'Company' THEN properties->'parent'
    END AS reference,

    properties->'addressEntity' AS addressEntity

FROM entities_datasets_small
;

SELECT * FROM entities_datasets_small WHERE schema = 'Directorship';
SELECT * FROM (SELECT properties->>'director' AS t FROM entities_datasets_small WHERE schema = 'Directorship') a WHERE t is not null


