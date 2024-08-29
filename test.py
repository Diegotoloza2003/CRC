import json
import requests
import mysql.connector
from mysql.connector import Error
from datetime import datetime

# Configuración de la base de datos
DB_CONFIG = {
    'database': 'biq360_reports_paitrade',
    'user': 'soporte',
    'password': 'biq360soporte01pAc*453',
    'host': '192.168.4.14',
    'port': 3306
}

# Configuración del servicio web
# token JUNIO 19-2024
WEB_SERVICE_URL = "https://tramitescrcom.gov.co/excluidosback/consultaMasiva/validarExcluidos"
TOKEN = "eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJzdWIiOiJDQzk0NDc0MjU0IiwianRpIjoiNzI1ODYiLCJyb2xlcyI6IlBST1ZFRURPUl9ERV9CSUVORVNfWV9TRVJWSUNJT1MiLCJpZEVtcHJlc2EiOjEwNzczNCwiaWRNb2R1bG8iOjQzMywicGVydGVuZWNlQSI6IkRERiIsInR5cGUiOiJleHRlcm5hbCIsIm5vbWJyZU1vZHVsbyI6IlJORSBMZXkgRGVqZW4gZGUgRnJlZ2FyIiwiaWF0IjoxNzE4OTE4MTA1LCJleHAiOjE3MzQ2ODYxMDV9.VKfLhOuyq38ZV2ljEMdqV1PEvQfv6a2OayxQ10xT9SQ"

# Función para hacer la consulta al servicio web
def consulta_rne(tipo, keys):
    headers = {
        "Content-Type": "application/json",
        "Authorization": f"Bearer {TOKEN}"
    }
    payload = {
        "type": tipo,
        "keys": keys
    }

    response = requests.post(WEB_SERVICE_URL, headers=headers, json=payload)
    
    if response.status_code == 200:
        try:
            return response.json()
        except ValueError:
            print("Error: Respuesta no es JSON")
            return None
    elif response.status_code == 401:
        print("Error: No Autorizado")
        return None
    else:
        try:
            error = response.json()
            print("Error:", error)
        except ValueError:
            print("Error:", response.status_code, response.text)
        return None

# Conexión a la base de datos
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    if conn.is_connected():
        print("Conexión exitosa a la base de datos")
except Error as e:
    print(f"Error al conectar a la base de datos: {e}")

cur = conn.cursor()

# Consultar un dato único en la base de datos
def obtener_dato_unico(consulta_sql):
    try:
        cur.execute(consulta_sql)
        result = cur.fetchone()
        if result:
            return result[0]
        else:
            print(f"No se encontró ningún dato para la consulta: {consulta_sql}")
            return None
    except Error as e:
        print(f"Error ejecutando la consulta: {e}")
        return None

# Obtener el estado actual en CRC_RESULTADO_TELEFONO
def obtener_estado_telefono(telefono):
    query = "SELECT estado_sms, estado_llamada FROM CRC_RESULTADO_TELEFONO WHERE telefono = %s"
    try:
        cur.execute(query, (telefono,))
        result = cur.fetchone()
        if result:
            return result
        else:
            return None
    except Error as e:
        print(f"Error obteniendo el estado del teléfono: {e}")
        return None

# Obtener el estado actual en CRC_RESULTADO_EMAIL
def obtener_estado_email(email):
    query = "SELECT estado FROM CRC_RESULTADO_EMAIL WHERE email = %s"
    try:
        cur.execute(query, (email,))
        result = cur.fetchone()
        if result:
            return result[0]
        else:
            return None
    except Error as e:
        print(f"Error obteniendo el estado del correo: {e}")
        return None

# Actualizar o insertar el resultado en CRC_RESULTADO_TELEFONO solo si hay un cambio en el estado
def actualizar_resultado_telefono(telefono, estado_sms, estado_llamada):
    estado_actual = obtener_estado_telefono(telefono)
    
    if estado_actual:
        # Comparar el estado actual con el nuevo estado
        if (estado_actual[0] != estado_sms) or (estado_actual[1] != estado_llamada):
            query = """
            INSERT INTO CRC_RESULTADO_TELEFONO (telefono, fecha_consulta, estado_sms, estado_llamada)
            VALUES (%s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                fecha_consulta = VALUES(fecha_consulta),
                estado_sms = VALUES(estado_sms),
                estado_llamada = VALUES(estado_llamada)
            """
            fecha_consulta = datetime.now()
            try:
                cur.execute(query, (telefono, fecha_consulta, estado_sms, estado_llamada))
                conn.commit()
                print(f"Actualizado resultado en CRC_RESULTADO_TELEFONO para teléfono: {telefono}")
            except Error as e:
                print(f"Error actualizando resultado en CRC_RESULTADO_TELEFONO: {e}")
        else:
            print(f"No se realizaron cambios en el estado del teléfono: {telefono}")
    else:
        query = """
        INSERT INTO CRC_RESULTADO_TELEFONO (telefono, fecha_consulta, estado_sms, estado_llamada)
        VALUES (%s, %s, %s, %s)
        """
        fecha_consulta = datetime.now()
        try:
            cur.execute(query, (telefono, fecha_consulta, estado_sms, estado_llamada))
            conn.commit()
            print(f"Insertado resultado en CRC_RESULTADO_TELEFONO para teléfono: {telefono}")
        except Error as e:
            print(f"Error insertando resultado en CRC_RESULTADO_TELEFONO: {e}")

# Insertar o actualizar resultado en CRC_RESULTADO_EMAIL solo si hay un cambio en el estado
def insertar_resultado_email(email, estado):
    estado_actual = obtener_estado_email(email)
    
    if estado_actual is not None:
        # Comparar el estado actual con el nuevo estado
        if estado_actual != estado:
            query = """
            INSERT INTO CRC_RESULTADO_EMAIL (email, fecha_consulta, estado)
            VALUES (%s, %s, %s)
            ON DUPLICATE KEY UPDATE
                fecha_consulta = VALUES(fecha_consulta),
                estado = VALUES(estado)
            """
            fecha_consulta = datetime.now()
            try:
                cur.execute(query, (email, fecha_consulta, estado))
                conn.commit()
                print(f"Actualizado resultado en CRC_RESULTADO_EMAIL para email: {email}")
            except Error as e:
                print(f"Error actualizando resultado en CRC_RESULTADO_EMAIL: {e}")
        else:
            print(f"No se realizaron cambios en el estado del correo: {email}")
    else:
        query = """
        INSERT INTO CRC_RESULTADO_EMAIL (email, fecha_consulta, estado)
        VALUES (%s, %s, %s)
        """
        fecha_consulta = datetime.now()
        try:
            cur.execute(query, (email, fecha_consulta, estado))
            conn.commit()
            print(f"Insertado resultado en CRC_RESULTADO_EMAIL para email: {email}")
        except Error as e:
            print(f"Error insertando resultado en CRC_RESULTADO_EMAIL: {e}")

def obtener_dato_unico(consulta_sql):
    try:
        cur.execute(consulta_sql)
        result = cur.fetchall()  # Usar fetchall() para asegurarte de que todos los resultados se procesen
        if result:
            return [r[0] for r in result]  # Retornar una lista de resultados
        else:
            print(f"No se encontró ningún dato para la consulta: {consulta_sql}")
            return []
    except Error as e:
        print(f"Error ejecutando la consulta: {e}")
        return []

# Obtener múltiples registros para la prueba
telefonos = obtener_dato_unico("SELECT telefono FROM biq360_reports_paitrade.CRC_Telefonos LIMIT 100")
correos = obtener_dato_unico("SELECT email FROM biq360_reports_paitrade.CRC_Email LIMIT 100")

# Asegúrate de cerrar el cursor después de cada consulta
cur.close()

# Reabre el cursor para las siguientes operaciones
cur = conn.cursor()

def procesar_prueba_multiple(telefonos, correos):
    resultados_procesados = {
        "telefonos": [],
        "correos": []
    }

    for telefono in telefonos:
        print(f"Procesando teléfono: {telefono}")
        resultado_tel = consulta_rne("TEL", [telefono])

        if resultado_tel:
            for res in resultado_tel:
                if res['llave'] == telefono:
                    opciones_contacto = res.get('opcionesContacto', {})
                    print(f"Resultado para teléfono: {telefono}")

                    estado_sms = 0
                    estado_llamada = 0

                    if not opciones_contacto.get('sms', True):
                        estado_sms = 1
                    if not opciones_contacto.get('llamada', True):
                        estado_llamada = 1

                    actualizar_resultado_telefono(telefono, estado_sms, estado_llamada)
                    resultados_procesados["telefonos"].append(res)
        else:
            print(f"No se encontró información para el teléfono: {telefono}. Desea ser contactado.")
            actualizar_resultado_telefono(telefono, 0, 0)

    for correo in correos:
        print(f"Procesando correo: {correo}")
        resultado_cor = consulta_rne("COR", [correo])

        if resultado_cor:
            for res in resultado_cor:
                if res['llave'] == correo:
                    opciones_contacto = res.get('opcionesContacto', {})
                    print(f"Resultado para correo: {correo}")

                    estado = 0
                    if not res.get('deseaSerContactado', True):
                        estado = 1

                    insertar_resultado_email(correo, estado)
                    resultados_procesados["correos"].append(res)
        else:
            print(f"No se encontró información para el correo: {correo}. Desea ser contactado.")
            insertar_resultado_email(correo, 0)

    return resultados_procesados

# Ejecutar el proceso de prueba con múltiples registros
datos_procesados = procesar_prueba_multiple(telefonos, correos)

# Imprimir el JSON en la consola
if datos_procesados:
    print(json.dumps(datos_procesados, indent=4))

# Cerrar la conexión a la base de datos
cur.close()
conn.close()
