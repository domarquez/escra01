import requests
from bs4 import BeautifulSoup
import psycopg2
import os
from datetime import datetime
import re
import time

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
    print(f"Longitud del texto HTML: {len(texto)} caracteres")  # Debug
    
    # Encuentra todos los bloques array(5) con sus posiciones
    array_blocks = re.finditer(r'array\(5\)\s*\{(?:.*?)\}', texto, re.DOTALL | re.IGNORECASE)
    
    if not array_blocks:
        print("No se encontraron bloques array. Muestra parcial del texto para debug:")
        print(texto[texto.find('array'):texto.find('array')+1000] if 'array' in texto else "No hay 'array' en el texto.")
        return []
    
    datos_lista = []
    soup = BeautifulSoup(texto, 'html.parser')
    nombre_divs = soup.find_all('div', class_='font-weight-bold bg-oscuro-1')
    
    for block_match in array_blocks:
        block = block_match.group()
        block_start = block_match.start()
        # Limpia el bloque
        clean_block = ' '.join(line.strip() for line in block.split('\n') if line.strip())
        print(f"Procesando bloque limpio: {clean_block[:100]}...")  # Debug
        
        un_match = re.search(r'\["un"\]=>\s*(?:int\(\s*(\d+)\s*\)|string\(\d+\)\s*"(\d+)")', clean_block, re.DOTALL | re.IGNORECASE)
        producto_id_match = re.search(r'\["producto_id"\]=>\s*int\(\s*(\d+)\s*\)', clean_block, re.DOTALL | re.IGNORECASE)
        fecha_match = re.search(r'\["fecha"\]=>\s*string\(\d+\)\s*"([^"]+)"', clean_block, re.DOTALL | re.IGNORECASE)
        saldo_match = re.search(r'\["saldo"\]=>\s*string\(\d+\)\s*"(\d+)"', clean_block, re.DOTALL | re.IGNORECASE)
        
        if un_match and producto_id_match and fecha_match and saldo_match:
            un_int, un_str = un_match.groups()
            un = int(un_int) if un_int else int(un_str)
            producto_id = int(producto_id_match.group(1))
            fecha = fecha_match.group(1)
            saldo = int(saldo_match.group(1))
            
            # Busca el nombre en el div más cercano después del bloque
            nombre = f"Estación {un}"  # Fallback
            for div in nombre_divs:
                div_start = texto.index(str(div))
                if block_start < div_start and div.get_text(strip=True):
                    nombre = div.get_text(strip=True)
                    break
            
            # Busca la ubicación cerca del nombre
            ubicacion_start = texto.find(str(nombre))
            if ubicacion_start != -1:
                ubicacion_match = re.search(r'location:\s*([A-Z\s\.,KM\d\-]+?)(?=\s*stock|\.)', texto[ubicacion_start:], re.DOTALL | re.IGNORECASE)
                ubicacion = ubicacion_match.group(1).strip() if ubicacion_match else "Ubicación no encontrada"
            else:
                ubicacion = "Ubicación no encontrada"
            
            # Estima vehículos por estación
            vehiculos_match = re.search(r'cantidad de vehiculos.*?(\d+\.?\d*)', texto[block_start:block_start+1000])
            vehiculos = float(vehiculos_match.group(1)) if vehiculos_match else 0
            
            tiempo_match = re.search(r'avanza cada (\d+) minutos', texto[block_start:block_start+1000])
            tiempo = int(tiempo_match.group(1)) if tiempo_match else 0
            
            stock_legible = f"{saldo:,} Lts."
            
            datos = {
                'estacion': nombre,
                'ubicacion': ubicacion,
                'producto_id': producto_id,
                'stock_litros': saldo,
                'stock_legible': stock_legible,
                'fecha_medicion': fecha,
                'vehiculos_estimados': vehiculos,
                'tiempo_cola_min': tiempo,
                'un_id': un
            }
            datos_lista.append(datos)
        else:
            print(f"Match failed in block: {clean_block}")
    
    print(f"Extraídos {len(datos_lista)} registros de stock.")
    return datos_lista

def guardar_en_neon(datos_lista):
    if not DATABASE_URL:
        print("Error: No hay DATABASE_URL.")
        return
    
    try:
        conn = psycopg2.connect(DATABASE_URL)
        cur = conn.cursor()
        
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
    while True:
        datos = extraer_datos(URL)
        if datos:
            print("Datos extraídos:", datos)
            guardar_en_neon(datos)
        else:
            print("No se extrajeron datos.")
        time.sleep(1800)  # Pausa de 30 minutos
