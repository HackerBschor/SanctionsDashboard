/* Indexes */
CREATE INDEX ON entities(LOWER(caption));
CREATE INDEX ON companies(LOWER(name));
CREATE INDEX ON entities(id);
CREATE INDEX ON entities_countries(id);

CREATE INDEX ON entities(LOWER(caption));
CREATE INDEX ON entities(schema);
CREATE INDEX ON entities_countries(source_country);

/* Join Industries */
UPDATE entities e SET industry = c.industry
FROM entities e1
JOIN companies c ON (LOWER(e1.caption) = LOWER(c.name))
WHERE e.id = e1.id AND e.schema = 'Company';

/* Create Country -sanctions-> Country table */
INSERT INTO entities_countries
    (id, caption, schema, target_country, source_country, first_seen, last_seen, last_change, target, industry)
SELECT * FROM (
    SELECT DISTINCT id, caption, schema,
        json_array_elements_text(COALESCE(properties->'country', properties->'jurisdiction')) AS target_country,
        publisher->>'country' as source_country,
        first_seen, last_seen, last_change, target, industry
    FROM (
        SELECT id, caption, schema, first_seen, last_seen, last_change, target, properties,
               json_array_elements_text(datasets) AS name, industry
        FROM entities
    ) e
    JOIN (SELECT * FROM datasets WHERE type <> 'external') d USING (name)
) f WHERE source_country IS NOT NULL AND target_country IS NOT NULL