@echo off
echo ======================================
echo CulturaTrip - Carga de datasets
echo ======================================

docker compose run --rm app python src/"New Model"/Paises_load_postgres.py
docker compose run --rm app python src/"New Model"/Comunidad_Autonomas_New_Model_load_postgres.py
docker compose run --rm app python src/"New Model"/Provincias_new_model_load_postgres.py
docker compose run --rm app python src/"New Model"/Islas_v2_new_model_load_postgres.py
docker compose run --rm app python src/"New Model"/Division_Politica_load_postgres.py
docker compose run --rm app python src/"New Model"/rel_municipio_isla_load_postgres.py
docker compose run --rm app python src/"New Model"/OpenstreetMap_load_postgres.py
docker compose run --rm app python src/"New Model"/Actividades_load_postgres.py
docker compose run --rm app python src/"New Model"/Alojamientos_load_postgres.py

echo ======================================
echo Carga completada
echo ======================================

pause