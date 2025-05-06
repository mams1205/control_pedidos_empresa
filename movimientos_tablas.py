import tkinter as tk
from tkinter import ttk
import customtkinter as ctk
from PIL import Image, ImageTk
from datetime import datetime, timedelta
import pandas as pd
from tkinter import ttk
from tkinter import messagebox
from model.querys_dao_2 import afacta, general_factc, impuestos_producto, insert_bita, insert_into_pedweb, lista_prod, lista_pedidos, max_cvedoc_factc, modif_exist_inve04, modif_exist_mult04, par_factc, query_busqueda_op, query_asurtir, query_op, query_acomprar, query_archivo_pedidos, query_ctrl_orden_produccion, salidas, insert_salidas, entradas, insert_entradas, select_cvebita, select_detalle, select_existg, select_ped_web, update_ctrl_05, insert_exist_entradas, list_of_names, guardar, variables_inventarios_entradas, requi_lista, ctrl_requi, foliosc05, web_movinve
from model.querys_dao_2 import query_descargar_pedidos, query_list_all_pedidos, query_insert_new, query_login, query_nuevo_usuario, query_desc_web, select_usuario, producido, materia_prima, variables_salidad_inve, variables_entradas_inve, consumo_nuevo, insert_exist_salidas, update_ctrl_04, almacen_04, requi_compra, insert_part_req,unidad_partida, update_estado_compras
from model.querys_dao_2 import agregar_productos_pp, listar_pp, seleccionar_pp,  eliminar_pp, pedi_05, calc_max_ltpd04,convertir_fecha, buscar_pedi_04, select_lista_pedimentos, insert_ltpd04, update_ltpd05, modificar_lote_nuevo, query_clave_admin, listar_users, eliminar_user, query_ctrl_pt_falta, des_product, inve_program, num_max_usuario, max_op, recetas, insert_compras
import pyodbc as podbc
from tkinter import filedialog
import os
import sys

from cgitb import reset

from pyparsing import col
from model.conexion_db import SQLServerConnector
from tkinter import messagebox
import pandas as pd
import numpy as np
from datetime import datetime
import pyodbc as podbc
from tabulate import tabulate
import os
import sys


def resource_path(relative_path):
    """ Get the absolute path to the resource, works for dev and for PyInstaller """
    try:
        # PyInstaller creates a temporary folder and stores path in _MEIPASS
        base_path = sys._MEIPASS
    except Exception:
        base_path = os.path.abspath(".")

    return os.path.join(base_path, relative_path)

def parameters():
    global servidor, database_04, database_05
    file_path = resource_path(f"conexion/conexion.csv")
    df_conexion = pd.read_csv(file_path)
    servidor = df_conexion.iloc[0]['servidor']
    database_04 = df_conexion.iloc[0]['base04']
    database_05 = df_conexion.iloc[0]['base05']
parameters()

def tablas ():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"SAE90Empre01"
        )]
    sql = f''' SELECT TABLE_NAME
                FROM INFORMATION_SCHEMA.TABLES
                WHERE TABLE_TYPE = 'BASE TABLE'
                '''
    sql_cuenta = ''' SELECT count(*) as row_count
                        from {table_name} '''
    

    try:
        # Establecer la conexión y crear el cursor
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        
        # Ejecutar consulta para obtener los nombres de las tablas
        cursor.execute(sql)
        result = cursor.fetchall()
        
        # Convertir el resultado a un DataFrame
        result = [list(row) for row in result]
        result_tablas = pd.DataFrame(result, columns=['table_name'])
        
        # Obtener la lista de tablas
        lista = result_tablas['table_name'].tolist()
        
        lista_cuentas = []
        
        for elemento in lista:
            # Crear la consulta dinámica para contar las filas de cada tabla
            query = sql_cuenta.format(table_name=elemento)
            cursor.execute(query)
            
            # Obtener el resultado del conteo
            result_cuentas = cursor.fetchone()
            row_count = result_cuentas[0]
            
            # Solo agregar tablas con filas mayores que 0
            if row_count > 0:
                lista_cuentas.append((elemento, row_count))
            lista_cuentas.append((elemento, result_cuentas[0]))  # Almacenar tabla y su conteo de filas
        
        pd.DataFrame(lista_cuentas).to_csv("movimientos_tabla.csv")

        
        # existg = exist_result[0]
        # exist_01 = exist_result[1]
        
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error existg"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

    return result

x = tablas()