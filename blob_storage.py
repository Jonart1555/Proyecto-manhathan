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

# Nombre del blob de bloqueo
LOCK_BLOB_NAME = "function_lock.json"

def load_all_tasks(task_blob_name):
    """
    Carga todas las tareas desde el archivo list_of_tasks_in_progress.json en Blob Storage.
    Si el archivo no existe o ocurre un error, retorna una lista vacía.
    """
    blob_client = container_client.get_blob_client(task_blob_name)
    try:
        tasks_json = blob_client.download_blob().readall().decode("utf-8")
        tasks = json.loads(tasks_json)
    except Exception as e:
        logging.warning(f"Error al cargar tareas desde {task_blob_name}: {e}")
        tasks = []
    return tasks

def save_all_tasks(tasks, task_blob_name):
    """
    Guarda la lista completa de tareas en el archivo JSON correspondiente en Blob Storage.
    """
    blob_client = container_client.get_blob_client(task_blob_name)
    blob_client.upload_blob(json.dumps(tasks, ensure_ascii=False, indent=4), overwrite=True)

def add_task(task, task_blob_name):
    """
    Agrega una nueva tarea a la lista de tareas.
    """
    tasks = load_all_tasks(task_blob_name)
    tasks.append(task)
    save_all_tasks(tasks, task_blob_name)

def delete_task(tid, task_blob_name):
    """
    Elimina una tarea del archivo JSON correspondient basado en su tid.
    """
    tasks = load_all_tasks(task_blob_name)
    tasks = [task for task in tasks if task.get("tid") != tid]
    save_all_tasks(tasks, task_blob_name)

def get_task(tid, task_blob_name):
    """
    Obtiene una tarea del archivo JSON por su tid.
    """
    tasks = load_all_tasks(task_blob_name)
    
    # Buscar coincidencia exacta en tid o en historical_tids
    for task in tasks:
        if task.get("tid") == tid:
            return task    
    return None

def update_task(updated_task, task_blob_name):
    """
    Actualiza una tarea existente en el archivo JSON.
    """
    tasks = load_all_tasks(task_blob_name)
    for i, task in enumerate(tasks):
        if task.get("tid") == updated_task.get("tid"):
            tasks[i] = updated_task
            break
    save_all_tasks(tasks, task_blob_name)

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
        
if __name__ == "__main__":
    release_lock()