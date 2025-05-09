-- Relacionar densidad de población con el número de estaciones de transporte por distrito

-- Primera pregunta 
SELECT d.nombre AS distrito, 
       d.densidad_poblacion, 
       COUNT(e.id) AS num_estaciones_transporte
FROM distritos d
LEFT JOIN estaciones_transporte e 
  ON d.id = e.distrito_id
GROUP BY d.id, d.nombre, d.densidad_poblacion
ORDER BY d.densidad_poblacion DESC;

-- Segunda pregunta

SELECT 
    station_origin_id,
    station_dest_id,
    SUM(total_viajes) AS total_viajes
FROM 
    rutas_users
GROUP BY 
    station_origin_id, station_dest_id
ORDER BY 
    total_viajes DESC
LIMIT 10;


SELECT 
    user_type,
    SUM(total_viajes) AS total_viajes,
    COUNT(DISTINCT station_origin_id || '-' || station_dest_id) AS rutas_distintas
FROM 
    rutas_users
GROUP BY 
    user_type;


SELECT 
    user_type,
    AVG(avg_duration_seconds) AS avg_duracion_segundos,
    AVG(avg_distance_km) AS avg_distancia_km
FROM 
    rutas_users
GROUP BY 
    user_type;


SELECT 
    user_type,
    station_origin_id,
    station_dest_id,
    SUM(total_viajes) AS total_viajes
FROM 
    rutas_users
GROUP BY 
    user_type, station_origin_id, station_dest_id
ORDER BY 
    user_type, total_viajes DESC;



