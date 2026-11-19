-- expect: Plan
-- says: input_file_name
-- tries: input_file_name(), which names the server's segment paths
SELECT input_file_name() AS f FROM token__transfer
