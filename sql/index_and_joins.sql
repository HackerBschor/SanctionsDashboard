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

UPDATE entities_countries ec SET industry = (SELECT industry FROM entities e WHERE e.id = ec.id)