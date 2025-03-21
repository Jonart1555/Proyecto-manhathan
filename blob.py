import os
import json
import logging
from datetime import datetime, timedelta
from dotenv import load_dotenv
from azure.storage.blob import BlobServiceClient



# Cargar variables de entorno
load_dotenv()

AZURE_STORAGE_CONNECTION_STRING = os.getenv("AZURE_STORAGE_CONNECTION_STRING")
AZURE_CONTAINER_NAME = os.getenv("AZURE_CONTAINER_NAME", "tasks")

if not AZURE_STORAGE_CONNECTION_STRING:
    raise ValueError("Error: La variable de entorno AZURE_STORAGE_CONNECTION_STRING no está definida.")
if not AZURE_CONTAINER_NAME:
    raise ValueError("Error: La variable de entorno AZURE_CONTAINER_NAME no está definida.")

blob_service_client = BlobServiceClient.from_connection_string(AZURE_STORAGE_CONNECTION_STRING)
container_client = blob_service_client.get_container_client(AZURE_CONTAINER_NAME)

# Intentar crear el contenedor si no existe
try:
    container_client.create_container()
except Exception as e:
    logging.info(f"El contenedor '{AZURE_CONTAINER_NAME}' ya existe o no se pudo crear: {e}")

# Nombre del blob que contendrá la lista de tareas
TASKS_BLOB_NAME = "list_of_tasks_in_progress.json"
# Nombre del blob de bloqueo
LOCK_BLOB_NAME = "function_lock.json"

def load_all_tasks():
    """
    Carga todas las tareas desde el archivo list_of_tasks_in_progress.json en Blob Storage.
    Si el archivo no existe o ocurre un error, retorna una lista vacía.
    """
    blob_client = container_client.get_blob_client(TASKS_BLOB_NAME)
    try:
        tasks_json = blob_client.download_blob().readall().decode("utf-8")
        tasks = json.loads(tasks_json)
    except Exception as e:
        logging.warning(f"Error al cargar tareas desde {TASKS_BLOB_NAME}: {e}")
        tasks = []
    return tasks

def save_all_tasks(tasks):
    """
    Guarda la lista completa de tareas en el archivo list_of_tasks_in_progress.json en Blob Storage.
    """
    blob_client = container_client.get_blob_client(TASKS_BLOB_NAME)
    blob_client.upload_blob(json.dumps(tasks, ensure_ascii=False, indent=4), overwrite=True)

def add_task(task):
    """
    Agrega una nueva tarea a la lista de tareas.
    """
    tasks = load_all_tasks()
    tasks.append(task)
    save_all_tasks(tasks)

def delete_task(tid):
    """
    Elimina una tarea del archivo list_of_tasks_in_progress.json basada en su tid.
    """
    tasks = load_all_tasks()
    tasks = [task for task in tasks if task.get("tid") != tid]
    save_all_tasks(tasks)

def get_task(tid):
    """
    Obtiene una tarea del archivo list_of_tasks_in_progress.json por su tid.
    Primero busca coincidencia exacta en el campo "tid" o en "historical_tids".
    Si no se encuentra, intenta buscar por current_tid y cliente.
    """
    tasks = load_all_tasks()
    
    # Buscar coincidencia exacta en tid o en historical_tids
    for task in tasks:
        if task.get("tid") == tid:
            return task
        historical_tids = task.get("historical_tids", [])
        if tid in historical_tids:
            return task

    # Si no se encontró, buscar por current_tid y cliente
    parts = tid.split("-")
    if len(parts) < 2:
        return None
    base_tid, cliente = parts[0], parts[1]
    
    for task in tasks:
        if task.get("current_tid") == base_tid and task.get("cliente") == cliente:
            return task
    
    return None

def update_task(updated_task):
    """
    Actualiza una tarea existente en el archivo list_of_tasks_in_progress.json.
    """
    tasks = load_all_tasks()
    for i, task in enumerate(tasks):
        if task.get("tid") == updated_task.get("tid"):
            tasks[i] = updated_task
            break
    save_all_tasks(tasks)

# Funciones para el bloqueo usando Blob Locking
def acquire_lock():
    """
    Intenta adquirir el bloqueo mediante la creación de un blob exclusivo.
    Retorna True si se adquiere el lock, False de lo contrario.
    """
    blob_client = container_client.get_blob_client(LOCK_BLOB_NAME)
    try:
        # Se intenta crear el blob sin sobreescribir (overwrite=False)
        blob_client.upload_blob(json.dumps({"locked": True, "timestamp": str(datetime.utcnow())}), overwrite=False)
        return True
    except Exception:
        # Si ya existe, se asume que otro proceso tiene el lock.
        return False

def release_lock():
    """
    Libera el bloqueo eliminando el blob de lock.
    """
    blob_client = container_client.get_blob_client(LOCK_BLOB_NAME)
    try:
        blob_client.delete_blob()
    except Exception as e:
        logging.warning(f"Error liberando lock: {e}")
        
        

# Nuevo blob para cache de estados finales
FINAL_STATUS_BLOB_NAME = "final_status_cache.json"

def load_final_status_cache():
    """Carga la caché de estados finales desde Blob Storage.
        Si el archivo no existe o falla, retorna un diccionario vacío.
    """
    blob_client = container_client.get_blob_client(FINAL_STATUS_BLOB_NAME)
    try:
        cache_json = blob_client.download_blob().readall().decode("utf-8")
        return json.loads(cache_json)
    except Exception as e:
        logging.warning(f"Cache vacía o error al cargar {FINAL_STATUS_BLOB_NAME}: {e}")
        return {}

def save_final_status_cache(cache):
    """Guarda la caché de estados finales en Blob Storage."""
    blob_client = container_client.get_blob_client(FINAL_STATUS_BLOB_NAME)
    blob_client.upload_blob(json.dumps(cache, ensure_ascii=False, indent=4), overwrite=True)

def actualizar_cache_final(tid, status, cliente):
    """Actualiza la caché de estados finales con el estado final de la tarea.
        Se establece una expiración de 30 minutos.
    """
    from datetime import datetime, timedelta
    if acquire_lock():
        try:
            cache = load_final_status_cache()
            cache[tid] = {
                "status": status,
                "cliente": cliente,
                "timestamp": datetime.utcnow().isoformat(),
                "expiration": (datetime.utcnow() + timedelta(minutes=30)).isoformat()
            }
            save_final_status_cache(cache)
        finally:
            release_lock()

def limpiar_cache_expirada():
    """Limpia las entradas expiradas de la caché de estados finales.
        Esta función puede ser llamada periódicamente (por ejemplo, mediante un Timer Trigger).
    """
    from datetime import datetime
    cache = load_final_status_cache()
    now = datetime.utcnow()
    cache_actualizada = {k: v for k, v in cache.items() if datetime.fromisoformat(v["expiration"]) > now}
    if len(cache) != len(cache_actualizada):
        save_final_status_cache(cache_actualizada)
        

if __name__ == "__main__":
    release_lock()