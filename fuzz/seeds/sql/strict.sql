SELECT div(CAST(1.005 AS DECIMAL(10,3)), 3, 4, 'half_even'), CAST(amount AS DECIMAL(10,0)), amount / 2, AVG(amount) FROM t GROUP BY amount
