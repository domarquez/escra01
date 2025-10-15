import requests
from bs4 import BeautifulSoup
import psycopg2
import os
from datetime import datetime
import re

# URL
URL = 'http://ec2-3-22-240-207.us-east-2.compute.amazonaws.com/guiasaldos/main/donde/134'

# DB
DATABASE_URL = os.getenv('DATABASE_URL')

def extraer_datos(url):
    response = requests.get(url)
    if response.status_code != 200:
        print(f"Error: Página no carga (código {response.status_code})")
        return []
    
    texto = response.text
    print(f"Longitud del texto HTML: {len(texto)} caracteres")  # Debug: confirma que carga
    
    # Regex más flexible: Captura arrays con campos clave, ignorando orden exacto y variaciones en string length
    # Busca 'array(5)' seguido de los 4 campos clave (un, producto_id, fecha, saldo)
    arrays = re.findall(r'array$$ 5 $$\s*\{\s*$$ .*?"un" $$=>\s*int$$ (\d+) $$\s*$$ .*?"producto_id" $$=>\s*int$$ (\d+) $$\s*$$ .*?"fecha" $$=>\s*string$$ \d+ $$\s*"([^"]+)"\s*$$ .*?"saldo" $$=>\s*string$$ \d+ $$\s*"(\d+)"\s*\}', texto, re.DOTALL | re.IGNORECASE)
    
    if not arrays:
        print("No se encontraron arrays. Muestra parcial del texto para debug:")
        print(texto[texto.find('array'):texto.find('array')+1000] if 'array' in texto else "No hay 'array' en el texto.")  # Debug
        return []
    
    datos_lista = []
    for match in arrays:
        un, producto_id, fecha, saldo_str = match
        saldo = int(saldo_str)
        
        # Extrae nombre y ubicación (busca cerca del un_id)
        nombre_match = re.search(rf'un$$ \s*{un}\s* $$.*?([A-Z\s]+?)(?=\s*,\s*location|\.)', texto, re.DOTALL | re.IGNORECASE)
        nombre = nombre_match.group(1).strip() if nombre_match else f"Estación {un}"
        
        ubicacion_match = re.search(rf'un$$ \s*{un}\s* $$.*?location:\s*([A-Z\s\.,KM\d\-]+?)(?=\s*stock|\.)', texto, re.DOTALL | re.IGNORECASE)
        ubicacion = ubicacion_match.group(1).strip() if ubicacion_match else "Ubicación no encontrada"
        
        # Stock legible
        stock_legible = f"{saldo:,} Lts."
        
        # Estimaciones (aproximadas del texto global)
        vehiculos_match = re.search(r'cantidad de vehiculos.*?(\d+\.?\d*)', texto)
        vehiculos = float(vehiculos_match.group(1)) if vehiculos_match else 0
        
        tiempo_match = re.search(r'avanza cada (\d+) minutos', texto)
        tiempo = int(tiempo_match.group(1)) if tiempo_match else 0
        
        datos = {
            'estacion': nombre,
            'ubicacion': ubicacion,
            'producto_id': int(producto_id),
            'stock_litros': saldo,
            'stock_legible': stock_legible,
            'fecha_medicion': fecha,
            'vehiculos_estimados': vehiculos,
            'tiempo_cola_min': tiempo,
            'un_id': int(un)
        }
        datos_lista.append(datos)
    
    print(f"Extraídos {len(datos_lista)} registros de stock.")
    return datos_lista

def guardar_en_neon(datos_lista):
    if not DATABASE_URL:
        print("Error: No hay DATABASE_URL.")
        return
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
        # Crea tabla si no existe
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
        
        # Inserta múltiples
        for datos in datos_lista:
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
        print(f"Guardados {len(datos_lista)} registros en Neon.")
    except Exception as e:
        print(f"Error al guardar: {e}")

if __name__ == "__main__":
    datos = extraer_datos(URL)
    if datos:
        print("Datos extraídos:", datos)  # Debug completo
        guardar_en_neon(datos)
    else:
        print("No se extrajeron datos.")
