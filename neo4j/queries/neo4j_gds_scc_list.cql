CALL gds.wcc.stream('graph', {})
YIELD nodeId, componentId
RETURN gds.util.asNode(nodeId).fingerprint, componentId
ORDER BY componentId, nodeId ASC;