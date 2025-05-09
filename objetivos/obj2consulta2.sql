-- Aqui mostraremos las querys que se han utilizado para responder a las preguntas planteadas en el objetivo 2.


-- Para ello, hemos utilizado un modelo multidmensional, donde las tablas de hechos son las que contienen los datos de las rutas y los usuarios y las tablas de dimensiones son las que contienen los datos de las estaciones y los distritos.

-- EL modelo multidimensional ya lo tenemos ubicado en PostregreSQL, para la ejecucion y prueba de las queries la hemos realizado 
-- en superset para aprovechar su uso intuitivo en el objectivo 3.

-- Primera  pregunta 
-- ¿Qué  rutas  de  BiciMAD  son  más  populares  entre  los  usuarios  y  cómo  varían  los patrones  de  uso  entre  usuarios  abonados  y  ocasionales?"


-- Top 10 rutas con más viajes entre estaciones.
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

-- Total de viajes y rutas únicas por tipo de usuario.
SELECT 
    user_type,
    SUM(total_viajes) AS total_viajes,
    COUNT(DISTINCT station_origin_id || '-' || station_dest_id) AS rutas_distintas
FROM 
    rutas_users
GROUP BY 
    user_type;


-- Promedio de duración y distancia de viajes por tipo de usuario.
SELECT 
    user_type,
    AVG(avg_duration_seconds) AS avg_duracion_segundos,
    AVG(avg_distance_km) AS avg_distancia_km
FROM 
    rutas_users
GROUP BY 
    user_type;


--Total de viajes por ruta y tipo de usuario, ordenado por tipo y viajes.
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


-- Segunda pregunta 
-- ¿Cómo  se  relaciona  la densidad de población de los distritos con la presencia de infraestructura de transporte público?"

SELECT d.nombre AS distrito, 
       d.densidad_poblacion, 
       COUNT(e.id) AS num_estaciones_transporte
FROM distritos d
LEFT JOIN estaciones_transporte e 
  ON d.id = e.distrito_id
GROUP BY d.id, d.nombre, d.densidad_poblacion
ORDER BY d.densidad_poblacion DESC;

