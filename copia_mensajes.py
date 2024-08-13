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

    print(f"Consultando {tipo} con payload: {payload}")

    response = requests.post(WEB_SERVICE_URL, headers=headers, json=payload)

    print("Contenido de response:")
    print(response.status_code)
    
    if response.status_code == 200:
        try:
            print("Respuesta exitosa recibida")
            print("Contenido de la respuesta JSON:")
            print(response.json())  # Añadido para ver la respuesta completa
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

# Actualizar la base de datos con los resultados del servicio web
def actualizar_base_datos(tabla, columna_llave, valor_llave, datos):
    if datos.get('deseaSerContactado', False):  # Default is False if key does not exist
        query = f"""
        UPDATE {tabla}
        SET opciones_contacto = %s, fecha_creacion = %s
        WHERE {columna_llave} = %s
        """
        print(f"Actualizando {tabla} para {valor_llave} con datos: {datos}")
        try:
            cur.execute(query, (str(datos['opcionesContacto']), datos['fechaCreacion'], valor_llave))
            conn.commit()
        except Error as e:
            print(f"Error actualizando la base de datos: {e}")
    else:
        print(f"La persona con {columna_llave} = {valor_llave} no desea ser contactada. No se realizará ninguna actualización.")

# Actualizar o insertar el resultado en CRC_RESULTADO_TELEFONO
def actualizar_resultado_telefono(telefono, estado_sms, estado_llamada):
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
        print(f"Actualizado resultado en CRC_RESULTADO_TELEFONO para telefono: {telefono}")
    except Error as e:
        print(f"Error actualizando resultado en CRC_RESULTADO_TELEFONO: {e}")

# Insertar resultado en CRC_RESULTADO_EMAIL
def insertar_resultado_email(email, estado):
    query = "INSERT INTO CRC_RESULTADO_EMAIL (email, fecha_consulta, estado) VALUES (%s, %s, %s)"
    fecha_consulta = datetime.now()
    try:
        cur.execute(query, (email, fecha_consulta, estado))
        conn.commit()
        print(f"Insertado resultado en CRC_RESULTADO_EMAIL para email: {email}")
    except Error as e:
        print(f"Error insertando resultado en CRC_RESULTADO_EMAIL: {e}")

# Proceso de prueba con un solo registro
def procesar_prueba_unico():
    consulta_telefono = "SELECT telefono FROM biq360_reports_paitrade.CRC_Telefonos LIMIT 1"
    telefono = obtener_dato_unico(consulta_telefono)

    consulta_email = "SELECT email FROM biq360_reports_paitrade.CRC_Email LIMIT 1"
    correo = obtener_dato_unico(consulta_email)
    
    if correo:
        correo = correo.strip()

    resultados_procesados = {
        "telefonos": [],
        "correos": []
    }

    if telefono:
        print(f"Procesando teléfono: {telefono}")
        resultado_tel = consulta_rne("TEL", [telefono])
        
        if resultado_tel:
            for res in resultado_tel:
                print(f"Procesando resultado: {res}")  # Añadido para ver cada resultado
                if res['llave'] == telefono:
                    opciones_contacto = res.get('opcionesContacto', {})
                    print(f"Resultado para teléfono: {telefono}")

                    # Calcular estado_sms y estado_llamada
                    estado_sms = 0  # Inicialmente suponemos que quiere ser contactado (estado=0)
                    estado_llamada = 0  # Mismo para llamadas

                    # Ahora verificamos si no desea ser contactado y cambiamos a 1 si es necesario
                    if not opciones_contacto.get('sms', True):  # False si NO desea ser contactado
                        estado_sms = 1
                    if not opciones_contacto.get('llamada', True):  # False si NO desea ser contactado
                        estado_llamada = 1

                    for canal, valor in opciones_contacto.items():
                        estado = "SI" if valor else "NO"
                        print(f"  - {canal}: {estado} desea ser contactado")

                    actualizar_base_datos('CRC_Telefonos', 'telefono', telefono, res)
                    
                    # Usa la función de actualización para manejar duplicados
                    actualizar_resultado_telefono(telefono, estado_sms, estado_llamada)
                    resultados_procesados["telefonos"].append(res)
        else:
            print(f"No se encontró información para el teléfono: {telefono}. Desea ser contactado.")
            # Si no se encontró información, se debe guardar el estado como 0 para ambos
            actualizar_resultado_telefono(telefono, 0, 0)

    if correo:
        print(f"Procesando correo: {correo}")
        resultado_cor = consulta_rne("COR", [correo])
        
        if resultado_cor:
            for res in resultado_cor:
                print(f"Procesando resultado: {res}")  # Añadido para ver cada resultado
                if res['llave'] == correo:
                    opciones_contacto = res.get('opcionesContacto', {})
                    print(f"Resultado para correo: {correo}")
                    
                    estado = 0  # Inicialmente suponemos que quiere ser contactado (estado=0)
                    if not res.get('deseaSerContactado', True):  # False si NO desea ser contactado
                        estado = 1
                        
                    insertar_resultado_email(correo, estado)
                    resultados_procesados["correos"].append(res)
        else:
            print(f"No se encontró información para el correo: {correo}. Desea ser contactado.")
            insertar_resultado_email(correo, 0)

    return resultados_procesados

# Ejecutar el proceso de prueba y obtener los datos en un diccionario
datos_procesados = procesar_prueba_unico()

# Imprimir el JSON en la consola
if datos_procesados:
    print(json.dumps(datos_procesados, indent=4))

# Cerrar la conexión a la base de datos
cur.close()
conn.close()
