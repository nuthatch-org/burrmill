-- expect: Plan
-- says: No function matches the given name and argument types 'sum(VARCHAR)'
-- tries: a type error DataFusion raises as an internal one, asking the caller to file a DataFusion bug
SELECT sum(l) AS s FROM labels
