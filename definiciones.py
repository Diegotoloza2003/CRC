import json
import requests
from mysql.connector import Error
from datetime import datetime

# Token y URL para el servicio web
WEB_SERVICE_URL = "https://tramitescrcom.gov.co/excluidosback/consultaMasiva/validarExcluidos"
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJDQzk0NDc0MjU0IiwianRpIjoiNzI1ODYiLCJyb2xlcyI6IlBST1ZFRURPUl9ERV9CSUVORVNfWV9TRVJWSUNJT1MiLCJpZEVtcHJlc2EiOjEwNzczNCwiaWRNb2R1bG8iOjQzMywicGVydGVuZWNlQSI6IkRERiIsInR5cGUiOiJleHRlcm5hbCIsIm5vbWJyZU1vZHVsbyI6IlJORSBMZXkgRGVqZW4gZGUgRnJlZ2FyIiwiaWF0IjoxNzE4OTE4MTA1LCJleHAiOjE3MzQ2ODYxMDV9.VKfLhOuyq38ZV2ljEMdqV1PEvQfv6a2OayxQ10xT9SQ"

def consulta_rne(tipo, keys):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }
    payload = {
        "type": tipo,
        "keys": keys
    }

    try:
        response = requests.post(WEB_SERVICE_URL, headers=headers, json=payload)
        response.raise_for_status()  # Lanza una excepción si la respuesta tiene un error HTTP
        return response.json()
    except requests.HTTPError as http_err:
        print(f"HTTP error occurred: {http_err}")
    except requests.RequestException as err:
        print(f"Error occurred: {err}")
    except ValueError:
        print("Error: Respuesta no es JSON válido")
    return None

def obtener_dato_unico(query, cursor):
    try:
        cursor.execute(query)
        result = cursor.fetchall()  # Obtener todos los resultados
        return [row[0] for row in result] if result else None  # Devolver una lista de resultados
    except Error as e:
        print(f"Error al ejecutar la consulta: {e}")
        return None


def actualizar_resultado_telefono(telefono, estado_sms, estado_llamada):
     if estado_sms == 0 and estado_llamada == 0:
      print(f"No se encontro informacion para el teléfono: {telefono}. Desea ser contactado.")

def insertar_resultado_email(correo, estado):
     if estado == 0:
      print(f"No se encontro informacion para el correo: {correo}. Desea ser contactado.")

def registrar_auditoria_procedimiento(cursor, conn, fecha_inicial, fecha_final, procedimiento, mensaje):
    query = """
    INSERT INTO biq360_reports_paitrade.Auditoria_Procedimientos
    (`FECHA INICIAL`, `FECHA FINAL`, Procedimiento, Mensaje)
    VALUES (%s, %s, %s, %s)
    """
    try:
        cursor.execute(query, (fecha_inicial, fecha_final, procedimiento, mensaje))
        conn.commit()
        print("Auditoria registrada correctamente en Auditoria_Procedimientos.")
    except Error as e:
        print(f"Error al registrar la auditoría: {e}")

procesados_tel = set()  # Conjunto para almacenar los teléfonos ya procesados
procesados_correo = set()  # Conjunto para almacenar los correos ya procesados

def procesar_bloque_telefonos(bloque_telefonos, resultados_procesados):
    for telefono in bloque_telefonos:
        print(f"Procesando teléfono: {telefono}")
        resultado_tel = consulta_rne("TEL", [telefono])

        if resultado_tel:
            for res in resultado_tel:
                if res['llave'] == telefono:
                    opciones_contacto = res.get('opcionesContacto', {})

                    estado_sms = 0
                    estado_llamada = 0

                    if not opciones_contacto.get('sms', True):
                        estado_sms = 1
                    if not opciones_contacto.get('llamada', True):
                        estado_llamada = 1

                    actualizar_resultado_telefono(telefono, estado_sms, estado_llamada)
                    resultados_procesados["telefonos"].append(res)
        else:
            if telefono not in procesados_tel:  # Verificación si ya ha sido procesado
                actualizar_resultado_telefono(telefono, 0, 0)
                procesados_tel.add(telefono)  # Agregar a los procesados

def procesar_bloque_correos(bloque_correos, resultados_procesados):
    for correo in bloque_correos:
        print(f"Procesando correo: {correo}")
        resultado_cor = consulta_rne("COR", [correo])

        if resultado_cor:
            for res in resultado_cor:
                if res['llave'] == correo:
                    opciones_contacto = res.get('opcionesContacto', {})

                    estado = 0
                    if not opciones_contacto.get('correo', True):
                        estado = 1

                    insertar_resultado_email(correo, estado)
                    resultados_procesados["correos"].append(res)
        else:
            if correo not in procesados_correo:  # Verificación si ya ha sido procesado
                insertar_resultado_email(correo, 0)
                procesados_correo.add(correo)  # Agregar a los procesados


def procesar_prueba_multiple(telefonos, correos):
    resultados_procesados = {
        "telefonos": [],
        "correos": []
    }

    # Procesar en bloques de 50
    total_telefonos = len(telefonos)
    total_correos = len(correos)

    for i in range(0, total_telefonos, 50):
        print(f"Procesando bloque de telefonos: {i + 1} a {min(i + 50, total_telefonos)}")
        procesar_bloque_telefonos(telefonos[i:i + 50], resultados_procesados)

    for i in range(0, total_correos, 50):
        print(f"Procesando bloque de correos: {i + 1} a {min(i + 50, total_correos)}")
        procesar_bloque_correos(correos[i:i + 50], resultados_procesados)

    return resultados_procesados
