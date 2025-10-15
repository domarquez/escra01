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

# Conversor estático: Mapeo de un_id a nombre, ubicación y coordenadas GPS
STATION_MAP = {
    100: {"name": "ALEMANA", "location": "AV. ALEMANA, 2DO ANILLO", "coords": (-17.7833, -63.1820)},
    105: {"name": "CHACO", "location": "AV. VIRGEN DE COTOCA, 2DO ANILLO", "coords": (-17.7750, -63.1950)},
    110: {"name": "ROYAL", "location": "AV. ROQUE AGUILERA ESQ CALLE ANGEL SANDOVAL NRO 3897 ZONA VILLA FATIMA", "coords": (-17.8140, -63.1540)},
    115: {"name": "EQUIPETROL", "location": "V. EQUIPETROL, 4TO ANILLO AL FRENTE DE EX - BUFALO PARK", "coords": (-17.7500, -63.1800)},
    120: {"name": "PARAGUA", "location": "AV. PARAGUA, 4TO ANILLO", "coords": (-17.7600, -63.1900)},
    135: {"name": "MONTECRISTO", "location": "AV. VIRGEN DE COTOCA 8VO ANILLO", "coords": (-17.7200, -63.1500)},
    200: {"name": "SUR CENTRAL", "location": "AV. SANTOS DUMONT, 2DO ANILLO", "coords": (-17.7900, -63.1700)},
    205: {"name": "BENI", "location": "AV. BENI, 2DO ANILLO", "coords": (-17.7850, -63.1850)},
    210: {"name": "LOPEZ", "location": "AV. BANZER, 7MO ANILLO", "coords": (-17.7300, -63.1600)},
    215: {"name": "VIRU VIRU", "location": "KM11 AL NORTE A LADO DE PLAY LAND PARK", "coords": (-17.6450, -63.1350)},
    225: {"name": "LA TECA", "location": "CARRETERA A COTOCA, ANTES DE LA TRANCA", "coords": (-17.8100, -63.2000)},
    300: {"name": "PIRAI", "location": "AV. ROCA Y CORONADO 3ER ANILLO", "coords": (-17.7700, -63.1750)},
    305: {"name": "CABEZAS", "location": "CARRETERA A CAMIRI", "coords": (-17.9000, -63.2500)},
    310: {"name": "PARAPETI", "location": "CAMIRI CARRETERA YACUIBA-SANTA CRUZ KM1 ZONA BARRIO LA WILLAMS", "coords": (-17.9500, -63.3000)},
    315: {"name": "BEREA", "location": "DOBLE VIA LA GUARDIA KM 8", "coords": (-17.8200, -63.2200)},
    400: {"name": "GASCO", "location": "AV. BANZER 3ER ANILLO", "coords": (-17.7800, -63.1650)},
}

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
            
            # Usa el conversor estático para nombre, ubicación y coords
            station_info = STATION_MAP.get(un, {"name": f"Estación {un}", "location": "Ubicación no encontrada", "coords": (0.0, 0.0)})
            nombre = station_info["name"]
            ubicacion = station_info["location"]
            lat, lon = station_info["coords"]
            
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
                'un_id': un,
                'latitud': lat,
                'longitud': lon
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
        
        # Asegúrate de que un_id sea único, eliminando duplicados si es necesario
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM pg_constraint WHERE conname = 'unique_un_id'
                ) THEN
                    -- Elimina duplicados, quedando el más reciente por un_id
                    DELETE FROM stocks
                    WHERE id NOT IN (
                        SELECT MAX(id)
                        FROM stocks
                        GROUP BY un_id
                    );
                    ALTER TABLE stocks ALTER COLUMN id RESTART WITH 1;
                    ALTER TABLE stocks ADD CONSTRAINT unique_un_id UNIQUE (un_id);
                END IF;
            END $$;
        """)
        
        # Agrega columnas latitud y longitud si no existen
        cur.execute("""
            DO $$ BEGIN
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='stocks' AND column_name='latitud'
                ) THEN
                    ALTER TABLE stocks ADD COLUMN latitud FLOAT;
                END IF;
                IF NOT EXISTS (
                    SELECT 1 FROM information_schema.columns 
                    WHERE table_name='stocks' AND column_name='longitud'
                ) THEN
                    ALTER TABLE stocks ADD COLUMN longitud FLOAT;
                END IF;
            END $$;
        """)
        
        # Usa UPSERT para actualizar o insertar
        for datos in datos_lista:
            cur.execute("""
                INSERT INTO stocks (estacion, ubicacion, producto_id, stock_litros, stock_legible, 
                                   fecha_medicion, vehiculos_estimados, tiempo_cola_min, un_id, latitud, longitud)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
                ON CONFLICT (un_id) DO UPDATE
                SET estacion = EXCLUDED.estacion,
                    ubicacion = EXCLUDED.ubicacion,
                    producto_id = EXCLUDED.producto_id,
                    stock_litros = EXCLUDED.stock_litros,
                    stock_legible = EXCLUDED.stock_legible,
                    fecha_medicion = EXCLUDED.fecha_medicion,
                    vehiculos_estimados = EXCLUDED.vehiculos_estimados,
                    tiempo_cola_min = EXCLUDED.tiempo_cola_min,
                    latitud = EXCLUDED.latitud,
                    longitud = EXCLUDED.longitud,
                    fecha_extraccion = CURRENT_TIMESTAMP;
            """, (datos['estacion'], datos['ubicacion'], datos['producto_id'], datos['stock_litros'],
                  datos['stock_legible'], datos['fecha_medicion'], datos['vehiculos_estimados'],
                  datos['tiempo_cola_min'], datos['un_id'], datos['latitud'], datos['longitud']))
        
        conn.commit()
        cur.close()
        conn.close()
        print(f"Guardados/Actualizados {len(datos_lista)} registros en Neon.")
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
