import mysql.connector
from datetime import datetime
import definiciones
from mysql.connector import Error

# Configuración de la base de datos
DB_CONFIG = {
    'database': 'biq360_reports_paitrade',
    'user': 'soporte',
    'password': 'biq360soporte01pAc*453',
    'host': '192.168.4.14',
    'port': 3306
}

# Conexión a la base de datos
try:
    conn = mysql.connector.connect(**DB_CONFIG)
    if conn.is_connected():
        print("Conexion exitosa a la base de datos")
except Error as e:
    print(f"Error al conectar a la base de datos: {e}")

cur = conn.cursor()

fecha_inicial = datetime.now()
fecha_final = fecha_inicial
procedimiento = "CRC"
mensaje = "INICIO"
# Registrar el inicio del procedimiento
definiciones.registrar_auditoria_procedimiento(cur, conn, fecha_inicial, fecha_final, procedimiento, mensaje)


# Consultar datos en la base de datos
telefonos = definiciones.obtener_dato_unico("SELECT distinct telefono FROM biq360_reports_paitrade.CRC_Telefonos", cur)
correos = definiciones.obtener_dato_unico("SELECT  distinct email FROM biq360_reports_paitrade.CRC_Email", cur)

if telefonos is None or correos is None:
    print("No se obtuvieron datos de telefonos o correos.")
    fecha_final = datetime.now()
    procedimiento = "CRC"
    mensaje = "ERROR consulta sin datos"
    # Registrar el inicio del procedimiento
    definiciones.registrar_auditoria_procedimiento(cur, conn, fecha_inicial, fecha_final, procedimiento, mensaje)
    # Manejar el caso según sea necesario
    exit()

# Ejecutar el proceso de CONSULTA EN EL CRC 
datos_procesados = definiciones.procesar_crc(cur,conn,telefonos, correos)

# Marcar la fecha final cuando el proceso ha terminado
fecha_final = datetime.now()
cantidad_telefonos_procesados = len(datos_procesados["telefonos"])
cantidad_correos_procesados = len(datos_procesados["correos"])
mensaje_final = f"FINAL: {cantidad_telefonos_procesados} teléfonos y {cantidad_correos_procesados} correos procesados."

# Registrar la finalización del procedimiento
definiciones.registrar_auditoria_procedimiento(cur, conn, fecha_inicial, fecha_final, procedimiento, mensaje_final)

# Cerrar el cursor y la conexión a la base de datos
cur.close()
conn.close()
