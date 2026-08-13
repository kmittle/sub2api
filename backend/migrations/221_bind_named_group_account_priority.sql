-- Keep the account_groups priority for the two private routing groups tied to
-- the canonical accounts.priority value.  The gateway scheduler already uses
-- the account-level value; these triggers keep the persisted association and
-- all future writes consistent with that same source of truth.

CREATE OR REPLACE FUNCTION sub2api_sync_named_group_account_priority()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
BEGIN
    UPDATE account_groups AS ag
    SET priority = a.priority
    FROM accounts AS a, groups AS g
    WHERE ag.account_id = a.id
      AND ag.account_id = NEW.id
      AND g.id = ag.group_id
      AND g.deleted_at IS NULL
      AND lower(g.name) IN ('my-codex', 'my-claude-code')
      AND ag.priority IS DISTINCT FROM a.priority;

    RETURN NEW;
END;
$$;

CREATE OR REPLACE FUNCTION sub2api_bind_named_group_account_priority()
RETURNS TRIGGER
LANGUAGE plpgsql
AS $$
DECLARE
    account_priority INTEGER;
    named_group BOOLEAN;
BEGIN
    SELECT a.priority,
           EXISTS (
               SELECT 1
               FROM groups AS g
               WHERE g.id = NEW.group_id
                 AND g.deleted_at IS NULL
                 AND lower(g.name) IN ('my-codex', 'my-claude-code')
           )
      INTO account_priority, named_group
      FROM accounts AS a
     WHERE a.id = NEW.account_id;

    IF named_group AND account_priority IS NOT NULL THEN
        NEW.priority := account_priority;
    END IF;

    RETURN NEW;
END;
$$;

DROP TRIGGER IF EXISTS trg_sub2api_sync_named_group_account_priority ON accounts;
CREATE TRIGGER trg_sub2api_sync_named_group_account_priority
AFTER UPDATE OF priority ON accounts
FOR EACH ROW
WHEN (OLD.priority IS DISTINCT FROM NEW.priority)
EXECUTE FUNCTION sub2api_sync_named_group_account_priority();

DROP TRIGGER IF EXISTS trg_sub2api_bind_named_group_account_priority ON account_groups;
CREATE TRIGGER trg_sub2api_bind_named_group_account_priority
BEFORE INSERT OR UPDATE OF account_id, group_id, priority ON account_groups
FOR EACH ROW
EXECUTE FUNCTION sub2api_bind_named_group_account_priority();

-- Repair existing rows in the same migration so the invariant is true before
-- the application starts serving traffic with the new trigger installed.
UPDATE account_groups AS ag
SET priority = a.priority
FROM accounts AS a, groups AS g
WHERE ag.account_id = a.id
  AND g.id = ag.group_id
  AND g.deleted_at IS NULL
  AND lower(g.name) IN ('my-codex', 'my-claude-code')
  AND ag.priority IS DISTINCT FROM a.priority;
