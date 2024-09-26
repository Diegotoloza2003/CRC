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

   # payload2 = json.dumps(payload) #cambiar separador del json de comillas simple ('') a doble ("")
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


# Actualizar o insertar el resultado en CRC_RESULTADO_TELEFONO
def actualizar_resultado_telefono(cursor,conn, telefono, estado_sms, estado_llamada):
    query ="""
    INSERT INTO CRC_RESULTADO_TELEFONO (telefono, fecha_consulta, estado_sms, estado_llamada)
    VALUES (%s, %s, %s, %s)
    ON DUPLICATE KEY UPDATE
        fecha_consulta = VALUES(fecha_consulta),
        estado_sms = VALUES(estado_sms),
        estado_llamada = VALUES(estado_llamada)
    """
    fecha_consulta = datetime.now()
    try:
        cursor.execute(query, (telefono, fecha_consulta, estado_sms, estado_llamada))
        conn.commit()
        print(f"Actualizado resultado en CRC_RESULTADO_TELEFONO para telefono: {telefono}")
    except Error as e:
        print(f"Error actualizando resultado en CRC_RESULTADO_TELEFONO: {e}")

# Insertar resultado en CRC_RESULTADO_EMAIL
def insertar_resultado_email(cursor,conn,  email, estado):
    query ="""
    INSERT INTO CRC_RESULTADO_EMAIL(email, fecha_consulta, estado)
    VALUES (%s, %s, %s)
    ON DUPLICATE KEY UPDATE
        fecha_consulta = VALUES(fecha_consulta),
        estado = VALUES(estado)
    """
    fecha_consulta = datetime.now()
    try:
        cursor.execute(query, (email, fecha_consulta, estado))
        conn.commit()
        print(f"Insertado resultado en CRC_RESULTADO_EMAIL para email: {email}")        
    except Error as e:
        print(f"Error insertando resultado en CRC_RESULTADO_EMAIL: {e}")


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

def procesar_bloque_telefonos(cursor, conn, bloque_telefonos, resultados_procesados):
    print(f"Procesando bloque de {len(bloque_telefonos)} telefonos")
    resultado_tel = consulta_rne("TEL", bloque_telefonos)
    if resultado_tel:
        for res in resultado_tel:
            telefono = res.get('llave')
            if telefono in bloque_telefonos:
                opciones_contacto = res.get('opcionesContacto', {})

                estado_sms = 0
                estado_llamada = 0

                if not opciones_contacto.get('sms', True):
                    estado_sms = 1
                if not opciones_contacto.get('llamada', True):
                    estado_llamada = 1

                if estado_llamada == 1 or estado_sms == 1:
                    actualizar_resultado_telefono(cursor, conn, telefono, estado_sms, estado_llamada)
                
                resultados_procesados["telefonos"].append(res)
                procesados_tel.add(telefono)


def procesar_bloque_correos(cursor,conn, bloque_correos, resultados_procesados):
    print(f"Procesando bloque de {len(bloque_correos)} correos")
    resultado_cor = consulta_rne("COR", bloque_correos)

    if resultado_cor:
        for res in resultado_cor:
            correo = res.get('llave')
            if correo in bloque_correos:        
                opciones_contacto = res.get('opcionesContacto', {})

                estado = 0
                if not opciones_contacto.get('correo', True):
                        estado = 1
                if estado == 1:
                        insertar_resultado_email(cursor,conn,correo, estado)
                        resultados_procesados["correos"].append(res)
                        procesados_correo.add(correo)

def procesar_crc(cursor, conn,telefonos, correos):
    resultados_procesados = {
        "telefonos": [],
        "correos": []
    }

    # Procesar en bloques de tamaño j

    total_telefonos = len(telefonos)
    total_correos = len(correos)
    j=5000 #Tamaño del bloque de registros

    for i in range(0, total_telefonos, j):
       print(f"Procesando bloque de telefonos: {i + 1} a {min(i + j, total_telefonos)}")
       procesar_bloque_telefonos(cursor, conn, telefonos[i:i + j], resultados_procesados)

    for i in range(0, total_correos, j):
        print(f"Procesando bloque de correos: {i + 1} a {min(i + j, total_correos)}")
        procesar_bloque_correos(cursor, conn, correos[i:i + j], resultados_procesados)

    return resultados_procesados
