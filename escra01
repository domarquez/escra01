import requests
from bs4 import BeautifulSoup
import psycopg2
import os
from datetime import datetime
import re  # Para extraer números con regex, por si el HTML varía

# URL de la página (cámbiala si hay más IDs, ej: /135 para otro producto)
URL = 'http://ec2-3-22-240-207.us-east-2.compute.amazonaws.com/guiasaldos/main/donde/134'

# Conexión a Neon (pon el DATABASE_URL en variables de entorno)
DATABASE_URL = os.getenv('postgresql://neondb_owner:npg_Vj2ROt6JrXHv@ep-tight-glitter-a8wysmyx-pooler.eastus2.azure.neon.tech/escra01?sslmode=require&channel_binding=require')  # Ej: postgresql://user:pass@ep-xxx.us-east-2.aws.neon.tech/db

# Función para extraer datos de la página
def extraer_datos(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error: No se pudo cargar la página (código {response.status_code})")
        return None
    
    soup = BeautifulSoup(response.text, 'html.parser')
    
    # Extrae el array PHP (busca el patrón 'array(5) {' y lo que sigue)
    array_match = re.search(r'array\(\d+\)\s*\{\s*\["id"\]=>\s*int\(\d+\)\s*\["un"\]=>\s*int\((\d+)\)\s*\["producto_id"\]=>\s*int\((\d+)\)\s*\["fecha"\]=>\s*string\("([^"]+)"\)\s*\["saldo"\]=>\s*string\("(\d+)"\)\s*\}', response.text)
    if array_match:
        un = array_match.group(1)  # ID de unidad/estación
        producto_id = array_match.group(2)
        fecha = array_match.group(3)
        saldo = int(array_match.group(4))  # Stock en litros
    else:
        print("No se encontró el array PHP. La página cambió?")
        return None
    
    # Extrae texto descriptivo (nombre estación, ubicación, etc.)
    # Busca patrones en el texto (ajusta si el HTML es fijo)
    texto = soup.get_text()
    nombre_match = re.search(r'PARAPETI|([A-Z\s]+)\.', texto)  # Asume nombre como "PARAPETI"
    nombre = nombre_match.group(1).strip() if nombre_match else "Estación Desconocida"
    
    ubicacion_match = re.search(r'CAMIRI CARRETERA YACUIBA-SANTA CRUZ KM1 ZONA BARRIO LA WILLAMS|([A-Z\s\.KM\d]+ZONA)', texto)
    ubicacion = ubicacion_match.group(1).strip() if ubicacion_match else "Ubicación no encontrada"
    
    # Stock legible (ya lo tenemos del array)
    stock_legible = f"{saldo:,} Lts."  # Formato con comas: 7,675 Lts.
    
    # Estimaciones (extrae números aproximados del texto)
    vehiculos_match = re.search(r'(\d+\.?\d*) vehículos', texto)
    vehiculos = float(vehiculos_match.group(1)) if vehiculos_match else 0
    
    tiempo_match = re.search(r'avanza cada: (\d+) minutos', texto)
    tiempo = int(tiempo_match.group(1)) if tiempo_match else 0
    
    return {
        'estacion': nombre,
        'ubicacion': ubicacion,
        'producto_id': producto_id,
        'stock_litros': saldo,
        'stock_legible': stock_legible,
        'fecha_medicion': fecha,
        'vehiculos_estimados': vehiculos,
        'tiempo_cola_min': tiempo,
        'un_id': un  # Para geolocalizar después si agregas una tabla de estaciones
    }

# Función para guardar en Neon (crea tabla si no existe)
def guardar_en_neon(datos):
    if not DATABASE_URL:
        print("Error: No hay DATABASE_URL configurado.")
        return
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Crea tabla si no existe (solo la primera vez)
        cur.execute("""
            CREATE TABLE IF NOT EXISTS stocks (
                id SERIAL PRIMARY KEY,
                estacion VARCHAR(255),
                ubicacion TEXT,
                producto_id INT,
                stock_litros INT,
                stock_legible VARCHAR(50),
                fecha_medicion TIMESTAMP,
                vehiculos_estimados FLOAT,
                tiempo_cola_min INT,
                un_id INT,
                fecha_extraccion TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        # Inserta los datos
        cur.execute("""
            INSERT INTO stocks (estacion, ubicacion, producto_id, stock_litros, stock_legible, 
                               fecha_medicion, vehiculos_estimados, tiempo_cola_min, un_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
        """, (datos['estacion'], datos['ubicacion'], datos['producto_id'], datos['stock_litros'],
              datos['stock_legible'], datos['fecha_medicion'], datos['vehiculos_estimados'],
              datos['tiempo_cola_min'], datos['un_id']))
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"Datos guardados: {datos['estacion']} tiene {datos['stock_litros']} Lts. de gasolina.")
    except Exception as e:
        print(f"Error al guardar en DB: {e}")

# Corre el scraper
if __name__ == "__main__":
    datos = extraer_datos(URL)
    if datos:
        print(datos)  # Para ver en consola
        guardar_en_neon(datos)
    else:
        print("No se pudieron extraer datos.")
