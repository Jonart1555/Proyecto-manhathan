import azure.functions as func
import logging
import requests
import json
import os
from dotenv import load_dotenv
from ulid import ULID
import jwt 
import threading
import time
from datetime import datetime, timezone
import random

from typing import List, Dict, Any, Optional

# Importar funciones de Blob Storage
from blob_storage import (
    add_task, get_task, delete_task, update_task,
    load_all_tasks, save_all_tasks, acquire_lock, release_lock
)

load_dotenv()

app = func.FunctionApp(http_auth_level=func.AuthLevel.ANONYMOUS)

def generate_jwt_token():
    """
    Genera un token JWT usando la clave secreta definida en la variable de entorno JWT_SECRET.
    """
    jwt_secret = os.getenv("JWT_SECRET")
    if not jwt_secret:
        logging.error("JWT_SECRET no definida en las variables de entorno.")
        
        return None
    payload = {"sub": "orquestador"}
    token = jwt.encode(payload, jwt_secret, algorithm="HS256")
    return token

def task_generator(vdom): 
    ulid = str(ULID())  # Genera un ULID único

    task_d = {
        "app": "tsmx-bloqueo-forti",
        "tid": f"{ulid}-{vdom}",
        "status": "pending",
        "created_at": datetime.now(timezone.utc).isoformat(),
        "updated_at": ""  #datetime.now(timezone.utc).isoformat()
    }
    return task_d

def find_item_by_id(json_data: List[Dict[str, Any]], tid: str) -> Optional[Dict[str, Any]]:
    for item in json_data:
        if item.get("tid") == tid:
            datos={
                "tid": item.get("tid"),
                "status": item.get("status")
            }
            return datos
    return None

def display_item_by_id(json_str: list, tid: str) -> Dict[str, Any]:
    try:
        result = find_item_by_id(json_str, tid)
        print(result)
        if result:
            return {
                "success": True,
                "data": result
            }
        else:
            return {
                "success": False,
                "error": f"No se encontró ningún elemento con tid: {tid}"
            }

    except json.JSONDecodeError as e:
        return {
            "success": False,
            "error": f"Error al parsear el JSON: {str(e)}"
        }
    except Exception as e:
        return {
            "success": False,
            "error": f"Error inesperado: {str(e)}"
        }

# ----------------------------
# Endpoint Orquestador
# ----------------------------
@app.route(route="orquestador")
def orquestador(req: func.HttpRequest) -> func.HttpResponse:
    try:
        logging.info("Inicio de orquestador de tareas")

        req_body = req.get_json()
        
        service  = req_body.get("service")
        vdom     = req_body.get("vdom")
        obj      = req_body.get("obj")
        gdr      = req_body.get("gdr")
        ticket   = req_body.get("ticket")
        action   = req_body.get("action")
      
        if not all([service, vdom, obj, gdr, ticket, action]):
            return func.HttpResponse(
                json.dumps({"error": "Faltan campos obligatorios"}),
                status_code=400,
                mimetype="application/json"
            )
        
        task_data = task_generator(vdom)
        task_data.update(
            {
                "vdom": vdom,
                "service": service,
                "obj": obj,
                "gdr": gdr,
                "ticket": ticket,
                "action": action
            }
        )
        add_task(task_data, f"bloqueos_{vdom}.json")
        logging.info(f"Tarea agregada: {task_data}")
        return func.HttpResponse(
            json.dumps(task_data),
            status_code=200,
            mimetype="application/json"
        )
    except Exception as e:
        logging.error(f"Error en orquestador: {e}")
        
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

# ----------------------------
# Endpoint Get Status
# ----------------------------
@app.route(route="get_status/{tid}", auth_level=func.AuthLevel.ANONYMOUS)
def get_status(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Inicio de get_status")
    try:
        tid = req.route_params.get("tid")
        
        if not tid:
            return func.HttpResponse(
                json.dumps({"error": "Parámetro 'tid' es requerido"}),
                status_code=404,
                mimetype="application/json"
            )
        id_to_find = tid
        print(id_to_find)
        tid_separado= id_to_find.split('-')
        vdom=tid_separado[1]
        print(vdom)
        name_file=f"bloqueos_{vdom}.json"
        sample_json = load_all_tasks(name_file)
        print(sample_json)
        result = display_item_by_id(sample_json, id_to_find)
        return func.HttpResponse(
                json.dumps(result),
                status_code=200,
                mimetype="application/json"
            )

    except Exception as e:
        logging.error(f"Error en get_status: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

# ----------------------------
# Endpoint Update Status
# ----------------------------
@app.route(route="update_status", auth_level=func.AuthLevel.ANONYMOUS)
def update_status(req: func.HttpRequest) -> func.HttpResponse:
    logging.info("Inicio de update_status")
    try:
        tid = req.params.get("tid")
        new_status = req.params.get("status")
        logging.info(tid)
        logging.info(new_status)
        
        if not tid or not new_status:
            return func.HttpResponse(
                json.dumps({"error": "Parametros 'tid' y 'status' son requeridos"}),
                status_code=400,
                mimetype="application/json"
            )
        if new_status not in ["pending", "executed", "failed"]:
            return func.HttpResponse(
                json.dumps({"error": "El estado debe ser 'pending', 'executed' o 'failed'"}),
                status_code=400,
                mimetype="application/json"
            )
        
        id_to_find = tid
        tid_separado= id_to_find.split('-')
        vdom=tid_separado[1]
        name_file=f"bloqueos_{vdom}.json"

        old_s_task = get_task(id_to_find, name_file)
        # Validacion de que existe
        if not old_s_task:
            return func.HttpResponse(
                json.dumps({"error": "Tarea no encontrada"}),
                status_code=404,
                mimetype="application/json"
            )
        
        old_s_task["status"] = new_status
        old_s_task["updated_at"] = datetime.now(timezone.utc).isoformat()
        update_task(old_s_task, name_file)

        return func.HttpResponse(
            json.dumps({
                "tid": tid,
                "status": new_status
            }),
            status_code=200,
            mimetype="application/json"
        )

    except Exception as e:
        logging.error(f"Error en get_status: {e}")
        return func.HttpResponse(
            json.dumps({"error": str(e)}),
            status_code=500,
            mimetype="application/json"
        )

# ----------------------------
# Endpoint Get Pending Tasks
# ----------------------------
@app.route(route="pending_tasks", auth_level=func.AuthLevel.ANONYMOUS)
def pending_tasks(req: func.HttpRequest) -> func.HttpResponse:
    logging.info('Inicio de pending_tasks.')
    IP_DEL_FIREWALL = os.getenv("IP_DEL_FIREWALL")
    TOKEN_DE_AUTENTICACION = os.getenv("TOKEN_DE_AUTENTICACION")
            
    vdom = req.params.get('vdom')
    if not vdom:
            return func.HttpResponse(
                json.dumps({"error": "Parametro 'vdom' es requerido"}),
                status_code=404,
                mimetype="application/json"
            )
    name_file=f"bloqueos_{vdom}.json"
    tasks = load_all_tasks(name_file)
    pending_tasks = [task for task in tasks if task.get("status") == "pending"]
    
    response= {
        "host": IP_DEL_FIREWALL,
        "token": TOKEN_DE_AUTENTICACION,
        "vdom": vdom,
        "data": pending_tasks
    }
    
    return func.HttpResponse(
        json.dumps(response),
        status_code=200,
        mimetype="application/json"
    )
    
        
    

# pending, failed, executed