-- One row per ad account, mapped to the client that owns it.
-- Replaces the old pattern of parsing client names out of a free-text
-- field with SPLIT(client_name, "|").
SELECT
  client_id,
  TRIM(client_name) AS client_name,
  client_type,
  platform,
  account_id
FROM raw.clients
