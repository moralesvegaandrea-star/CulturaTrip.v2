@echo off
echo ======================================
echo CulturaTrip - Vistas, cambios y ML
echo ======================================

echo.
echo Primera pasada: crear todas las estructuras...
echo.

docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/05_new_views.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/07_new_changes.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/08_alter_tables.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/09_ML_views.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/10_auth.sql

echo.
echo Segunda pasada: resolver dependencias cruzadas...
echo.

docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/07_new_changes.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/08_alter_tables.sql
docker exec -i culturatrip_db psql -U culturatrip -d culturatrip -f /docker-entrypoint-initdb.d/09_ML_views.sql

echo.
echo ======================================
echo Vistas y cambios completados
echo ======================================

pause