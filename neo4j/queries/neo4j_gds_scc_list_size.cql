MATCH (u:Relay)
RETURN u.componentId AS Component, count(*) AS ComponentSize
ORDER BY ComponentSize DESC