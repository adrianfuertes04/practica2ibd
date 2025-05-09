-- Relacionar densidad de población con el número de estaciones de transporte por distrito
SELECT d.nombre AS distrito, 
       d.densidad_poblacion, 
       COUNT(e.id) AS num_estaciones_transporte
FROM distritos d
LEFT JOIN estaciones_transporte e 
  ON d.id = e.distrito_id
GROUP BY d.id, d.nombre, d.densidad_poblacion
ORDER BY d.densidad_poblacion DESC;
