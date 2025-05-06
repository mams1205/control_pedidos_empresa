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

def lista_prod(folio):
# Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    lista_productos = []
    # Example query
    sql = f'''SELECT P.Folio, P.skuvari, P.desvari, SUM(P.cant) AS cant, ISNULL(I.EXIST, 0) AS Exi, I.STOCK_MIN AS StMin
                    FROM SlicTe.dbo.STe_Pedidos P 
                    LEFT JOIN {database_04}.dbo.MULT04 I ON P.SkuVari COLLATE Modern_Spanish_CI_AS = I.CVE_ART
                    WHERE P.Folio = '{folio}' AND I.CVE_ALM = 1
                    GROUP BY P.Folio, P.skuvari, P.desvari, I.EXIST, I.STOCK_MIN'''

        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_productos = cursor.fetchall()
        lista_productos = [list(row) for row in lista_productos]
        # for row in rows:
        #     lista_productos.append(row)
        # Close the cursor and connection
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_productos

def lista_pedidos(sku_value, folio):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]
    
    lista_pedidos = []
    # Example query with proper string formatting
    sql = f'''SELECT nopedido, skuvari, desvari, cant, origen 
             FROM [SlicTe].[dbo].[STe_Pedidos]
             WHERE skuvari = '{sku_value}' AND folio = '{folio}'
             '''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_pedidos = cursor.fetchall()
        lista_pedidos = [list(row) for row in lista_pedidos]
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_pedidos

def query_descargar_pedidos(fecha): #descarga pedidos de SAE
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    lista_descarga_pedidos = []
    # Example query
    sql = f'''SELECT
                    P.CVE_DOC AS nopedido, 
                    '' AS cveprod, 
                    PD.CVE_ART AS skuvari,
                    I.DESCR AS desvari,
                    '' AS estatus,
                    '' AS stenvio,
                    P.FECHA_DOC AS fecha,
                    PD.CANT AS cant
               FROM 
                    FACTP04 P,
                    PAR_FACTP04 PD,
                    INVE04 I
                WHERE
                    CAST(P.FECHA_DOC AS DATE) = CONVERT(DATE, ?, 103) AND
                    P.CVE_DOC = PD.CVE_DOC AND
                    PD.CVE_ART = I.CVE_ART AND
                    P.STATUS <> 'C' AND
                    SUBSTRING(PD.CVE_ART, 1, 2) IN ('TS', 'TE', 'MR')'''
        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, fecha)
        lista_descarga_pedidos = cursor.fetchall()
        lista_descarga_pedidos = [list(row) for row in lista_descarga_pedidos]
        # for row in rows:
        #     lista_productos.append(row)
        # Close the cursor and connection
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_descarga_pedidos

def query_list_all_pedidos():
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    lista_all_pedidos = []
    # Example query
    sql = f'''SELECT 
                    [nopedido] ,[cveprod] ,[desprod] ,[skuvari] ,[desvari] ,[estatus] ,[stenvio] ,[fecha]
                    ,[cant] ,[origen] ,[fechaalta] ,[folio]
              FROM 
                    [SlicTe].[dbo].[STe_Pedidos]'''
        # Run the example query for each database
    try:
        first_connector = connectors[0]

        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_all_pedidos = cursor.fetchall()
        lista_all_pedidos = [list(row) for row in lista_all_pedidos]
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_all_pedidos

def query_desc_web():
       # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    list_descr = []
    # Example query
    sql = f'''SELECT DISTINCT CVE_ART, DESCR
                FROM INVE04'''
        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        list_descr = cursor.fetchall()
        list_descr = [list(row) for row in list_descr]
        cursor.close()
        connection.close()

        df_descr = pd.DataFrame(list_descr, columns = ["skuvari", "desvari"])

    except Exception as e:
        titulo = "Error en query"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

    return df_descr

def query_insert_new(lista_data):
       # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    # Example query
    sql = f''' SET ANSI_WARNINGS OFF;

                INSERT INTO  [SlicTe].[dbo].[STe_Pedidos] 
                    ([nopedido] ,[cveprod] ,[desprod] ,[skuvari] ,[desvari] ,[estatus] ,[stenvio] ,[fecha]
                    ,[cant] ,[origen] ,[fechaalta] ,[folio])
              VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
              
              SET ANSI_WARNINGS ON
              '''
        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in lista_data:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[7] = pd.to_datetime(row[7]).strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row[7]) else None
            row[10] = pd.to_datetime(row[10]).strftime('%Y-%m-%d %H:%M:%S') if pd.notnull(row[10]) else None

            # Ensure `cant` is a float
            row[8] = float(row[8]) if pd.notnull(row[8]) else None

            # Ensure `folio` is an integer
            row[11] = int(row[11]) if pd.notnull(row[11]) else None

            # Handle NaN values correctly by setting them to None
            row = [None if (isinstance(x, float) and np.isnan(x)) or x == 'nan' else x for x in row]

            cursor.execute(sql, tuple(row))
        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error en query ste_pedidos"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
        
def query_asurtir(folio):
     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]
    
    lista_asurtir = []
    # Example query with proper string formatting
    sql = f'''SELECT Folio,  origen, skuvari, desvari, SUM(cantnec) AS cant, stMin 
             FROM [SlicTe].[dbo].[STe_aSurtir]
             WHERE Folio = '{folio}'
             GROUP BY [Folio], [origen], [skuvari], [desvari], [stMin]
             ORDER BY [skuvari] ASC'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_asurtir = cursor.fetchall()
        lista_asurtir = [list(row) for row in lista_asurtir]
        cursor.close()
        connection.close()
      

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_asurtir

def query_op(folio):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]
    
    lista_op = []
    # Example query with proper string formatting
    sql = f'''SELECT OP, SUM(cant) AS cant ,skuvari, desvari, lote, consumo, Folio
              FROM [SlicTe].[dbo].[STe_OP]
              WHERE Folio = '{folio}'
              GROUP BY [OP],[Folio], [skuvari], [desvari], [lote], [consumo]
              ORDER BY [skuvari] ASC;'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_op = cursor.fetchall()
        lista_op = [list(row) for row in lista_op]
        cursor.close()
        connection.close()
      

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_op

def query_acomprar(folio):
     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        ),
         SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    lista_acomprar = []
    # Example query with proper string formatting
    sql = f'''SELECT sf.[Folio], sf.[skuvari], sf.[desvari], SUM(sf.[cant]) AS cantidad_comprar, i.[EXIST] AS exist, i.[STOCK_MIN] AS STOCK_MIN
              FROM [SlicTe].[dbo].[STe_Falta] sf
              JOIN (SELECT [CVE_ART], [EXIST], [STOCK_MIN]
              FROM {database_05}.dbo.INVE05) i ON sf.[skuvari] COLLATE Latin1_General_BIN = i.[CVE_ART] COLLATE Latin1_General_BIN
              WHERE sf.Folio = '{folio}'
              GROUP BY sf.[Folio], sf.[skuvari], sf.[desvari], i.[EXIST], i.[STOCK_MIN]
             ORDER BY  sf.[skuvari] ASC'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_acomprar = cursor.fetchall()
        lista_acomprar = [list(row) for row in lista_acomprar]
        cursor.close()
        connection.close()
      

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_acomprar

def query_login(usuario, password):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    # Example query
    sql = f'''SELECT [U_Nom],
                     [U_Pas]
               FROM 
                    [SlicTe].[dbo].[STe_Usuarios]
                WHERE
                    [U_Nom] = ? AND
                    [U_Pas] = ? '''
        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (usuario, password))
        result = cursor.fetchone()
        cursor.close()
        connection.close()

        if result:
            return True
        else:
            return False
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error query_login"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def num_max_usuario():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    
    # Example query
    sql = f'''SELECT MAX(U_NUM)  FROM [SlicTe].[dbo].[STe_Usuarios]'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)

        v_max = cursor.fetchone()[0]

        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

    return v_max

def query_nuevo_usuario(lista_nuevo_usuario):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    # Example query
    sql = f'''INSERT INTO  [SlicTe].[dbo].[STe_Usuarios] 
                    ([U_Nom] ,[U_Niv] ,[U_Pas], [U_Num])
              VALUES(?, ?, ?, ?)'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in lista_nuevo_usuario:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)
            cursor.execute(sql, row[0], row[1], row[2], row[3])
        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error en query"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def query_exitmp(folio):
    # Create a SQLServerConnector instance
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
    STe_ExiTMP = []
    STe_pedidos = []
    materia_prima = []
    # STe_OP = []
    # Example query with proper string formatting
    sql_delete = f'''DELETE FROM [SlicTe].[dbo].[STe_ExiTMP];'''

    sql_insert = f'''INSERT INTO [SlicTe].[dbo].[STe_ExiTMP] (folio, skuvari, desvari, cant, exist, stMin)
                    SELECT 
                    P.Folio, 
                    P.skuvari, 
                    P.desvari, 
                    SUM(ISNULL(P.cant, 0)) AS cant, 
                    AVG(ISNULL(I.EXIST, 0)) AS exist, 
                    AVG(ISNULL(I.STOCK_MIN, 0)) AS stMin
                    FROM 
                    STe_Pedidos P 
                    LEFT JOIN 
                    {database_04}.dbo.MULT04 I 
                    ON 
                    P.skuvari COLLATE Modern_Spanish_CI_AS = I.CVE_ART
                    WHERE 
                    P.Folio = ? AND CVE_ALM = 1
                    GROUP BY 
                    P.Folio, P.skuvari, P.desvari'''
    
    sql_select_pedidos = f'''Select Folio, origen, nopedido, skuvari, desvari, cant
	                         from STe_Pedidos 
                             where Folio = ?
	                         Order by skuvari'''
    
    sql_materia = f''' Select P.CVE_ART, P.CVE_PROD, I.DESCR, (P.CANTIDAD), I.EXIST, I.STOCK_MIN
           From {database_05}.dbo.KITS05 P, {database_05}.dbo.INVE05 I
            Where P.CVE_PROD = I.CVE_ART;'''
    
    try:
        first_connector = connectors[0]
        with first_connector.connect() as connection:
            with connection.cursor() as cursor:
                cursor.execute(sql_delete)
                cursor.execute(sql_insert, folio)
                connection.commit()

                cursor.execute("SELECT * FROM [SlicTe].[dbo].[STe_ExiTMP] ORDER BY skuvari")
                STe_ExiTMP = cursor.fetchall()
                STe_ExiTMP = [list(row) for row in STe_ExiTMP]

                cursor.execute(sql_select_pedidos, folio)
                STe_pedidos = cursor.fetchall()
                STe_pedidos = [list(row) for row in STe_pedidos]

                cursor.execute(sql_materia)
                materia_prima = cursor.fetchall()
                materia_prima = [list(row) for row in materia_prima]
        cursor.close()
        connection.close()
                
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

    try:    
        dfexi_temp = pd.DataFrame(STe_ExiTMP, columns = ["folio", "skuvari", "desvari", "cant", "exist", "stmin"])
        df_pedidos = pd.DataFrame(STe_pedidos, columns = ["folio", "origen", "nopedido", "skuvari", "desvari", "cant"])
        df_materia_prima = pd.DataFrame(materia_prima, columns = ["CVE_ART", "CVE_PROD", "DESCR", "CANTIDAD", "EXIST", "STOCK_MIN"])
        df_OP = pd.DataFrame(columns = ["OP", "cant", "skuvari", "desvari", "lote", "consumo", "folio", "nopedido", "origen", "N_S"])
    

        temp_skuvari = None
        a_prod = pd.DataFrame(columns = ["folio", "origen", "nopedido", "skuvari", "desvari", "cant_nec", "stmin"])
        a_surtir = pd.DataFrame(columns = ["folio", "origen", "nopedido", "skuvari", "desvari", "cant_nec", "stmin"])
        
        for i, row in df_pedidos.iterrows():
            folio_ped_value = row['folio']
            origen_ped_value = row['origen']
            nopedido_ped_value = row['nopedido']
            skuvari_ped_value = row['skuvari']
            desvari_ped_value = row['desvari']
            cant_ped_value = row['cant']
            
            if skuvari_ped_value != temp_skuvari:
                temp_skuvari = skuvari_ped_value

                criteria_exist = dfexi_temp[dfexi_temp['skuvari'] == temp_skuvari]
                
                if not criteria_exist.empty:
                    v_exist = criteria_exist['exist'].values[0]
                    v_stmin = criteria_exist['stmin'].values[0]
                else:
                    v_exist = 0
                    v_stmin = 0

            if v_exist < cant_ped_value:
                if v_exist == 0:
                # Inserta el registro del SKU a la lista de Produccion (aProd)
                    a_prod = a_prod._append({
                        'folio': folio_ped_value,
                        'origen': origen_ped_value,
                        'nopedido': nopedido_ped_value,
                        'skuvari':skuvari_ped_value,
                        'desvari':desvari_ped_value,
                        'cant_nec':cant_ped_value,
                        'stmin': v_stmin
                    }, ignore_index = True)
                    
                if v_exist > 0:
                    va_prod = cant_ped_value - v_exist
                # Inserta para produccion el registro del SKU con lo que hizo falta para surtir (vaProd)
                    a_prod = a_prod._append({
                        'folio': folio_ped_value,
                        'origen': origen_ped_value,
                        'nopedido': nopedido_ped_value,
                        'skuvari':skuvari_ped_value,
                        'desvari':desvari_ped_value,
                        'cant_nec':va_prod,
                        'stmin': v_stmin
                    }, ignore_index = True)
                #Inserta para surtir el registro del SKU con lo que alcanzó para surtir (vExist)
                    a_surtir = a_surtir._append({
                        'folio': folio_ped_value,
                        'origen': origen_ped_value,
                        'nopedido': nopedido_ped_value,
                        'skuvari': skuvari_ped_value,
                        'desvari': desvari_ped_value,
                        'cant_nec': v_exist,
                        'stmin': v_stmin
                    }, ignore_index = True)
                #Asigna 0 a la existencia    
                    v_exist = 0

            if v_exist>=cant_ped_value:
                #Asigna a Existencia el valor que queda al restar la Cantidad Pedida
                v_exist = v_exist-cant_ped_value
                
                a_surtir = a_surtir._append({
                    'folio': folio_ped_value,
                    'origen': origen_ped_value,
                    'nopedido': nopedido_ped_value,
                    'skuvari': skuvari_ped_value,
                    'desvari': desvari_ped_value,
                    'cant_nec': cant_ped_value,
                    'stmin': v_stmin
                    }, ignore_index = True)
    except Exception as e:
        titulo = "Contactar a Soporte IATI, Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

    return a_prod, a_surtir, df_materia_prima, df_OP

def num_lote():
    # Part 1: Extract the last two digits of the current year
    current_year = datetime.now().year
    year_str = str(current_year)
    last_two_digits = year_str[2:]

    # Part 2: Calculate the number of days since December 31 of the previous year
    current_date = datetime.now()
    previous_year_dec_31 = datetime(current_year - 1, 12, 31)
    days_since_dec_31 = (current_date - previous_year_dec_31).days
    days_str = f"{days_since_dec_31:03}"

    # Combine both parts
    lote = last_two_digits + days_str
    return lote

def caducidad():
    # Part 1: Extract the current month as a two-digit string
    current_month = datetime.now().month
    month_str = f"{current_month:02}"  # Ensures two-digit format

    # Part 2: Calculate the year 2 years from now
    current_year = datetime.now().year
    future_year = current_year + 2
    future_year_str = str(future_year)

    # Part 3: Concatenate the month and future year with a '/' separator
    caducidad = month_str + '/' + future_year_str
    return caducidad

def max_op():

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    
    # Example query
    sql_v= f''' select isnull(max(op),0) + 1 from [SlicTe].[dbo].[STe_OP]'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_v)

        v_max = cursor.fetchone()[0]

        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return v_max

def compra_falta(folio):

    df_aprod, df_asurtir, df_materia_prima, df_OP = query_exitmp(folio)
    


    df_aprod_agg = df_aprod.groupby(['folio', 'origen', 'skuvari', 'desvari']).agg({'cant_nec': 'sum'}).reset_index()
    
    # max_op = df_OP["OP"].max()
    v_nextOP = max_op()
    v_lote = num_lote()
    v_caducidad = caducidad()

    a_falta = pd.DataFrame(columns = ["folio", "sku_vari", "desvari", "cant", "exist", "ban","N_S"])
    
    for i, row in df_aprod_agg.iterrows():
        folio_prod_value = row['folio']
        skuvari_prod_value = row['skuvari']
        skuvari_prod_value = skuvari_prod_value.strip()
        cant_prod_value = row['cant_nec']
        origen_prod_value = row['origen']
        desvari_prod_value = row['desvari']

        # Filter df_materia_prima based on SKU
        df_materia_prima_filter = df_materia_prima[df_materia_prima['CVE_ART'] == str(skuvari_prod_value)].copy()
    
        # Calculate the required quantity by multiplying by cant_prod_value
        df_materia_prima_filter['CANTIDAD'] *= cant_prod_value
        # print(f"Filtered df_materia_prima: {df_materia_prima_filter.head()}")

        v_ban = "S"
        for index, mat_row in df_materia_prima_filter.iterrows():
            cant_materia_value = mat_row['CANTIDAD']
            exist_materia_value = mat_row['EXIST']
            stmin_value = mat_row['STOCK_MIN']
            descr_value = mat_row['DESCR']
            cve_prod_value = mat_row['CVE_PROD']

            

            if exist_materia_value < cant_materia_value or (exist_materia_value - cant_materia_value) <= stmin_value:
            
                if (exist_materia_value - cant_materia_value) <= stmin_value:
                 
                    v_ban = "M"
                    Ns = "N"
                
                # Append the row to a_falta DataFrame
                    a_falta = a_falta._append({
                        'folio': folio_prod_value,
                        'sku_vari': cve_prod_value, # or 'CVE_ART' if that's what you intend
                        'desvari': descr_value,
                        'cant': cant_materia_value,
                        'exist': exist_materia_value,
                        'ban': v_ban,
                        'N_S': Ns
                    }, ignore_index=True)
                
                if exist_materia_value < cant_materia_value and v_ban != "M":

                    v_ban = "F"
                    a_falta = a_falta._append({
                        'folio': folio_prod_value,
                        'sku_vari': cve_prod_value, #no es la cve ART?
                        'desvari': descr_value,
                        'cant': cant_materia_value,
                        'exist': exist_materia_value,
                        'ban': v_ban,
                        'N_S': Ns
                        }, ignore_index = True)
                    
        if v_ban == "S":
                produccion_state = "Por_producir"
                df_OP = df_OP._append({
                    'OP': v_nextOP,
                    'cant': cant_prod_value,
                    'skuvari':skuvari_prod_value,
                    'desvari': desvari_prod_value,
                    'lote':v_lote,
                    'consumo': v_caducidad,
                    'folio': folio_prod_value,
                    'origen':origen_prod_value,
                    'N_S': produccion_state
                }, ignore_index = True)
    
    #run the function recetas to know all the sku that have a recipe
    receta = recetas()
    #create an empty list            
    receta_df = []

    #iterate over each row of the df in the column skuvari
    for index, row in df_OP.iterrows():
        if row['skuvari'] in receta:
            receta_df.append('CON_RECETA')
        else:
            receta_df.append('SIN_RECETA')
    #add the results of the previous loop to the df
    df_OP["Receta"] = receta_df
    #tipo de OP
    df_OP["tipo"] = "Sistema"

    return df_aprod, df_asurtir,a_falta, df_OP

def insert_procedure_3(folio):
    df_aprod, df_asurtir, df_falta, df_OP = compra_falta(folio)

    #change data type
    df_aprod['folio'] = df_aprod['folio'].astype(int)
    df_aprod['origen'] = df_aprod['origen'].astype(str)
    df_aprod['nopedido'] = df_aprod['nopedido'].astype(str)
    df_aprod['skuvari'] = df_aprod['skuvari'].astype(str)
    df_aprod['desvari'] = df_aprod['desvari'].astype(str)
    df_aprod['cant_nec'] = df_aprod['cant_nec'].astype(float)
    df_aprod['stmin'] = df_aprod['stmin'].astype(float)

      # Convert to native Python types
    df_aprod = df_aprod.astype({
        'folio': 'int64',
        'origen': 'object',
        'nopedido': 'object',
        'skuvari': 'object',
        'desvari': 'object',
        'cant_nec': 'float64',
        'stmin': 'float64'
    })

    lista_aprod =  [tuple(x) for x in df_aprod.to_records(index=False)]

    #change data type
    df_asurtir['folio'] = df_asurtir['folio'].astype(int)
    df_asurtir['origen'] = df_asurtir['origen'].astype(str)
    df_asurtir['nopedido'] = df_asurtir['nopedido'].astype(str)
    df_asurtir['skuvari'] = df_asurtir['skuvari'].astype(str)
    df_asurtir['desvari'] = df_asurtir['desvari'].astype(str)
    df_asurtir['cant_nec'] = df_asurtir['cant_nec'].astype(float)
    df_asurtir['stmin'] = df_asurtir['stmin'].astype(float)

      # Convert to native Python types
    df_asurtir = df_asurtir.astype({
        'folio': 'int64',
        'origen': 'object',
        'nopedido': 'object',
        'skuvari': 'object',
        'desvari': 'object',
        'cant_nec': 'float64',
        'stmin': 'float64'
    })

    lista_asurtir =  [tuple(x) for x in df_asurtir.to_records(index=False)]

    #change data type
    df_falta['folio'] = df_falta['folio'].astype(int)
    df_falta['sku_vari'] = df_falta['sku_vari'].astype(str)
    df_falta['desvari'] = df_falta['desvari'].astype(str)
    df_falta['cant'] = df_falta['cant'].astype(int)
    df_falta['exist'] = df_falta['exist'].astype(int)
    df_falta['ban'] = df_falta['ban'].astype(str)
    df_falta['N_S'] = df_falta['N_S'].astype(str)

    

      # Convert to native Python types
    df_falta = df_falta.astype({
        'folio': 'int64',
        'sku_vari': 'object',
        'desvari': 'object',
        'cant': 'int64',
        'exist': 'int64',
        'ban': 'object',
        'N_S': 'object'
    })

    lista_falta =  [tuple(x) for x in df_falta.to_records(index=False)]

    #change data type
    df_OP['OP'] = df_OP['OP'].astype(int)
    df_OP['cant'] = df_OP['cant'].astype(int)
    df_OP['skuvari'] = df_OP['skuvari'].astype(str)
    df_OP['desvari'] = df_OP['desvari'].astype(str)
    df_OP['lote'] = df_OP['lote'].astype(str)
    df_OP['consumo'] = df_OP['consumo'].astype(str)
    df_OP['folio'] = df_OP['folio'].astype(int)
    df_OP['nopedido'] = df_OP['nopedido'].astype(str)
    df_OP['origen'] = df_OP['origen'].astype(str)
    df_OP['N_S'] = df_OP['N_S'].astype(str)
    df_OP['Receta'] = df_OP['Receta'].astype(str)
    df_OP['tipo'] = df_OP['tipo'].astype(str)

      # Convert to native Python types
    df_OP = df_OP.astype({
        'OP': 'int64',
        'cant': 'int64',
        'skuvari': 'object',
        'desvari': 'object',
        'lote': 'object',
        'consumo': 'object',
        'folio': 'int64',
        'nopedido': 'object',
        'origen': 'object',
        'N_S':'object',
        'Receta':'object',
        'tipo':'object'
    })

    lista_OP =  [tuple(x) for x in df_OP.to_records(index=False)]

    

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    
    # Example query
    sql_aprod = f'''INSERT INTO  [SlicTe].[dbo].[STe_aProd] 
                    ([folio] ,[origen] ,[nopedido] ,[skuvari] ,[desvari] ,[cantnec] ,[stMin])
              VALUES(?, ?, ?, ?, ?, ?, ?)'''
    
    sql_asurtir = f'''INSERT INTO  [SlicTe].[dbo].[STe_aSurtir] 
                    ([folio] ,[origen] ,[nopedido] ,[skuvari] ,[desvari] ,[cantnec] ,[stMin])
              VALUES(?, ?, ?, ?, ?, ?, ?)'''
    
    sql_falta = f'''INSERT INTO  [SlicTe].[dbo].[STe_Falta] 
                    ([folio] ,[skuvari] ,[desvari] ,[cant] ,[exist] ,[ban], [N_S])
              VALUES(?, ?, ?, ?, ?, ?, ?)'''
    
    sql_OP = f'''INSERT INTO  [SlicTe].[dbo].[STe_OP] 
                    ([OP] ,[cant] ,[skuvari] ,[desvari] ,[lote] ,[consumo], [folio], [nopedido], [origen], [N_S], [receta], [tipo])
              VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in lista_aprod:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[0] = int(row[0]) 


            cursor.execute(sql_aprod, tuple(row))

        for row in lista_asurtir:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[0] = int(row[0]) 
            cursor.execute(sql_asurtir, tuple(row))

        for row in lista_falta:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[0] = int(row[0])
            row[3] = int(row[3]) 
            row[4] = int(row[4])  

            cursor.execute(sql_falta, tuple(row))

        for row in lista_OP:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[0] = int(row[0])
            row[1] = int(row[1]) 
            row[6] = int(row[6])  

            cursor.execute(sql_OP, tuple(row))

        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error en query insert_procedure3"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def query_busqueda_op(folio):
    v_aprod, v_asurtir, v_falta, v_op = query_condicion(folio)
    respaldo_exitmp(folio)

    if all(var == 0 for var in [v_aprod, v_asurtir, v_falta, v_op]):
       
        #todo el proceso de las ordenes de aprod, afalta, asurtir, y OP
        insert_procedure_3(folio)
        
        #aqui se agregan los productos terminados que no se surtieron por falta de mp
        PT_falta(folio)
        connectors = [
            SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
            )]
    
        lista_op = []
        # Example query
        sql = f'''SELECT OP, Folio, cant, skuvari, desvari, lote, consumo
                    FROM SlicTe.dbo.STe_OP 
                    WHERE Folio = ? '''

        # Run the example query for each database
        try:
            first_connector = connectors[0]
            connection = first_connector.connect()
            cursor = connection.cursor()
            cursor.execute(sql, folio)
            lista_op = cursor.fetchall()
            lista_op = [list(row) for row in lista_op]

            # Close the cursor and connection
            cursor.close()
            connection.close()
        
        except Exception as e:
            titulo = "Error"
            mensaje = f"Error: {str(e)}"
            messagebox.showerror(titulo, mensaje)
        messagebox.showinfo("","Se ejecutó orden de producción")
        return lista_op
    else:
        connectors = [
            SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
            )]
    
        lista_op = []
        # Example query
        sql = f'''SELECT OP, Folio, cant, skuvari, desvari, lote, consumo
                    FROM SlicTe.dbo.STe_OP 
                    WHERE Folio = ? '''

        # Run the example query for each database
        try:
            first_connector = connectors[0]
            connection = first_connector.connect()
            cursor = connection.cursor()
            cursor.execute(sql, folio)
            lista_op = cursor.fetchall()
            lista_op = [list(row) for row in lista_op]

            # Close the cursor and connection
            cursor.close()
            connection.close()

        except Exception as e:
            titulo = "Error"
            mensaje = f"Error: {str(e)}"
            messagebox.showerror(titulo, mensaje)
        messagebox.showinfo("","Proceso de produccíon ejecutado previamente, solo se muestran resultados")
        return lista_op

def query_condicion(folio):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    

    # Example query
    sql_aprod_count = f'''SELECT Count (*)
                    FROM SlicTe.dbo.STe_aProd 
                    WHERE Folio = ? '''
    sql_asurtir_count = f'''SELECT Count (*)
                    FROM SlicTe.dbo.STe_aSurtir
                    WHERE Folio = ? '''
    sql_falta_count = f'''SELECT Count (*)
                    FROM SlicTe.dbo.STe_Falta
                    WHERE Folio = ? '''
    sql_op_count = f'''SELECT Count (*)
                    FROM SlicTe.dbo.STe_OP
                    WHERE Folio = ? '''
    

        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_aprod_count, folio)

        v_aprod = cursor.fetchone()[0]

        cursor.execute(sql_asurtir_count, folio)
        v_asurtir = cursor.fetchone()[0]

        cursor.execute(sql_falta_count, folio)
        v_falta = cursor.fetchone()[0]

        cursor.execute(sql_op_count, folio)
        v_op = cursor.fetchone()[0]

        # Close the cursor and connection
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return v_aprod, v_asurtir, v_falta, v_op

def query_archivo_pedidos(folio):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    lista_archivo_pedidos = []
    # Example query with proper string formatting
    # sql = f'''Select 'VTAWEB '+ltrim(rtrim(nopedido))+' {datetime.now().year}' as documento,
    #                   cant,
    #                   ltrim(rtrim(skuvari)) as Clave,
    #                   isnull(ULT_COSTO,0) costo,
    #                   '      61' concepto, convert(date, getdate()) as fecha,
    #                   '        1' Almacen  
    #         from SlicTe.dbo.STe_Pedidos left join {database_04}.dbo.INVE04 on SkuVari collate Modern_Spanish_CI_AS = CVE_ART
    #         where folio = ? and origen = 'WEB' 
	# 	   Order by Clave ASC'''

    sql = f'''  SELECT 
                    'VTAWEB ' + LTRIM(RTRIM(nopedido)) AS documento,
                    'WEB' AS cliente,
                    STUFF(LTRIM(RTRIM(nopedido)), 1, 1, '') AS clave,
                    cant,
                    LTRIM(RTRIM(skuvari)) AS SKUVARI,
                    ISNULL(PRECIO, 0) AS PRECIO,
                    ISNULL(i.ULT_COSTO, 0) AS Costo,
                    '1' AS Almacen
                FROM SlicTe.dbo.STe_Pedidos AS sp
                LEFT JOIN {database_04}.dbo.PRECIO_X_PROD04 AS pxp
                    ON sp.SkuVari COLLATE Modern_Spanish_CI_AS = pxp.CVE_ART
                LEFT JOIN {database_04}.dbo.INVE04 AS i
                    ON sp.SkuVari COLLATE Modern_Spanish_CI_AS = i.CVE_ART
                WHERE sp.folio = ? 
                AND sp.origen = 'WEB' 
                AND pxp.CVE_PRECIO = 15
                ORDER BY SKUVARI ASC;
                '''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, folio)
        lista_archivo_pedidos = cursor.fetchall()
        lista_archivo_pedidos = [list(row) for row in lista_archivo_pedidos]
        cursor.close()
        connection.close()


    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_archivo_pedidos

###insert pedidos web en tabla
def insert_into_pedweb(folio, lista_ped_web):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    folios_web = []
    sql_select = f'''SELECT DISTINCT(folio) FROM STe_PedWEB'''
    sql_insert = f'''INSERT INTO  [SlicTe].[dbo].[STe_PedWeb]
                    ([folio] ,[documento] ,[cliente] ,[clave] ,[cantidad] ,[skuvari] ,[precio], [costo] ,[fecha] ,[almacen], [band])
                    VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_select)
        folios_web = cursor.fetchall()
        folios_web = [item for sublist in folios_web for item in sublist]
        print(folios_web, folio)
        if int(folio) not in folios_web :
            for row in lista_ped_web:
            # Ensure the row is a list and convert the fields accordingly
                row = list(row)
                cursor.execute(sql_insert,tuple(row))
            connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    
def select_ped_web(folio):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    lista_ped_web = []

    # sql = f'''  SELECT * FROM SlicTe.dbo.STe_PedWeb where folio = ? order by documento asc'''
    sql = f'''  SELECT 
                    folio
                    ,(sum(folio)/folio) as total_partidas
                    ,documento
                    ,cliente
                    ,clave
                    ,round(sum(precio),2) as  cant_total
                    ,fecha
                    ,almacen
                    ,band
                FROM SlicTe.dbo.STe_PedWeb
                WHERE folio = ?
                Group by documento, folio, fecha, cliente, clave, almacen, band'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, folio)
        lista_ped_web = cursor.fetchall()
        lista_ped_web = [list(row) for row in lista_ped_web]
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

    return lista_ped_web

def select_detalle(clave_documento):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    lista_detalle_web = []

    sql = f'''  SELECT 
                    folio,
                    documento,
                    skuvari,
                    cantidad,
                    round(precio,2),
                    round(costo,2)
                FROM SlicTe.dbo.STe_PedWeb 
                WHERE documento = ? 
                order by skuvari asc'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, clave_documento)
        lista_detalle_web = cursor.fetchall()
        lista_detalle_web = [list(row) for row in lista_detalle_web]

        df_detalle = pd.DataFrame(lista_detalle_web, columns = ["folio", "documento", "skuvari", "cantidad", "precio","costo"])
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

    return lista_detalle_web

def max_cvedoc_factc():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]

    sql_select_max = f'''SELECT
                            MAX(CVE_DOC),
                            MAX(DAT_MOSTR)   
                        FROM 
                            {database_04}.dbo.FACTC04
                        WHERE 
                            SERIE = 'STAND.'
                            OR
                            SERIE = ''
                        '''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_select_max)
        result_max = cursor.fetchall()
        result_max = [item for sublist in result_max for item in sublist]
        
        cve_doc = result_max[0]
        dat_mostr = int(result_max[1])+1


        cve_doc = str(int(cve_doc)+1).zfill(10)
        cve_doc = ' ' * 10 + cve_doc 
        
        # #data frame que se va a insertar en factc con la 
        # factc_df = pd.DataFrame({'tip_doc': ['C'], 'cve_doc': [cve_doc], 'cve_clpv':[pedido['cliente']], 'status':['O'],
        #                         'cve_pedido': [pedido['documento']], 'fecha_doc': [datetime.now()], 'fecha_ent':[datetime.now()],'fecha_ven':[datetime.now()],
        #                         #sacar valores de los impuestos por partidas y sumarlos
        #                         'imp_tot1':[0], 'imp_tot2':[0], 'imp_tot3':[0], 'imp_tot4':[0], 'des_fin':[0],
        #                         'com_tot':[0],'num_moned':[1], 'tipcamb':[1], 'primerpago':[0],
        #                         'rfc':[""], 'folio':[int(cve_doc)], 'serie':['STAND.'], 'escfd':['N'],
        #                         'num_alma':[1], 'act_cxc': ['S'], 'act_coi': ['N'], 'can_tot': [pedido['precio_total']],
        #                         'cve_vend':[""], 'des_tot':[0], 'num_pagos':[1],'dat_envio':[0],
        #                         ###clave bita sacarlo de la bitacora???
        #                         'contado':['N'],'dat_mostr':[dat_mostr], 'cve_bita':[99999999], 'bloq':['N'],
        #                         'fecha_elab':[datetime.now()], 'ctlpol':[0], 'cve_obs':[0], 'enlazado':['O'],
        #                         'tip_doc_e':['E'], 'des_tot_porc':[0], 
        #                         #importe -> suma del precio mas impuestos
        #                         'importe':[tot_impuestos], 
        #                         #uuid y version sinc
        #                         'uuid':['xxxxxxx'], 'version_sinc':[datetime.now()],
        #                         'tip_fac':['C'], 'imp_tot8':[0], 'imp_tot7':[0], 'imp_tot6':[0], 'imp_tot5':[0]
                                # })
        # factc_df = factc_df.reset_index(drop=True)
        


        
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error existg"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

    return cve_doc

def impuestos_producto(cve_art):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    sql = f'''  SELECT 
                    I.CVE_ESQIMPU, 
                    IMPU.IMPUESTO1,
                    IMPU.IMP1APLICA,
                    IMPU.IMPUESTO2,
                    IMPU.IMP2APLICA,
                    IMPU.IMPUESTO3,
                    IMPU.IMP3APLICA,
                    IMPU.IMPUESTO4,
                    IMPU.IMP4APLICA

                FROM 
                    (
                        SELECT 
                            CVE_ESQIMPU
                        FROM 
                            {database_04}.dbo.INVE04
                        WHERE 
                            CVE_ART = ?
                    ) AS I
                LEFT JOIN 
                    {database_04}.dbo.IMPU04 as IMPU
                ON 
                    I.CVE_ESQIMPU = IMPU.CVE_ESQIMPU
                '''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, cve_art)
        resultado = cursor.fetchall()
        # resultado = [int(item) for sublist in resultado for item in sublist]

        impuestos = [list(pd.to_numeric(row)) for row in resultado]

        impuestos_df = pd.DataFrame(impuestos, columns = ["CVE_ESQIMPU", "IMPUESTO1", "IMP1APLICA", "IMPUESTO2", "IMP2APLICA",
                                                          "IMPUESTO3", "IMP3APLICA", "IMPUESTO4", "IMP4APLICA"])

    except Exception as e:
        titulo = "Error impuestos"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    
    return impuestos_df

def par_factc(cve_doc,contador, df_partidas_pedido, impuestos_df):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    tot_imp1 = (int(df_partidas_pedido['cantidad'])*pd.to_numeric(df_partidas_pedido['precio']))*(impuestos_df['IMPUESTO1'].iloc[0]/100)
    tot_imp2 = (int(df_partidas_pedido['cantidad'])*pd.to_numeric(df_partidas_pedido['precio']))*(impuestos_df['IMPUESTO2'].iloc[0]/100)
    tot_imp3 = (int(df_partidas_pedido['cantidad'])*pd.to_numeric(df_partidas_pedido['precio']))*(impuestos_df['IMPUESTO3'].iloc[0]/100)
    tot_imp4 = (int(df_partidas_pedido['cantidad'])*pd.to_numeric(df_partidas_pedido['precio']))*(impuestos_df['IMPUESTO4'].iloc[0]/100)


    par_factc_df = pd.DataFrame({'cve_doc':[cve_doc], 'num_par':[contador], 'cve_art':[df_partidas_pedido['skuvari']], 'cantidad':[df_partidas_pedido['cantidad']],
                                 'pxs':[df_partidas_pedido['cantidad']], 'precio':[df_partidas_pedido['precio']], 'costo':[df_partidas_pedido['costo']], 'impu1':[impuestos_df['IMPUESTO1'].iloc[0]],
                                  'impu2':[impuestos_df['IMPUESTO2'].iloc[0]], 'impu3':[impuestos_df['IMPUESTO3'].iloc[0]], 'impu4':[impuestos_df['IMPUESTO4'].iloc[0]], 'imp1apla':[impuestos_df['IMP1APLICA'].iloc[0]],
                                  'imp2apla':[impuestos_df['IMP2APLICA'].iloc[0]], 'imp3apla':[impuestos_df['IMP3APLICA'].iloc[0]], 'imp4apla':[impuestos_df['IMP4APLICA'].iloc[0]], 'totimp1':[tot_imp1],
                                  'totimp2':[tot_imp2], 'totimp3':[tot_imp3], 'totimp4':[tot_imp4],
                                  'desc1':[0],'desc2':[0], 'desc3':[0], 'comi':[0],
                                   'actinv':['N'],'num_alm':[1],'polit_apli':[""],'tip_cam':[1],
                                   'uni_venta':['pz'], 'tipo_prod':['P'], 'cve_obs':[0], 'reg_serie':[0],
                                   'e_ltpd':[0], 'tipo_elem':['N'],'tot_partida':[pd.to_numeric(df_partidas_pedido['cantidad'])*pd.to_numeric(df_partidas_pedido['precio'])], 
                                   'totimp8':[0], 'totimp7':[0], 'totimp6':[0], 'totimp5':[0]})
    
    list_parfactc_df = [tuple(x) for x in par_factc_df.to_records(index=False)] 
    
    sql = f'''  INSERT INTO 
                    {database_04}.dbo.PAR_FACTC04
                    (CVE_DOC, NUM_PAR, CVE_ART, CANT, PXS, PREC, COST, IMPU1, IMPU2, IMPU3, IMPU4,
                    IMP1APLA, IMP2APLA, IMP3APLA, IMP4APLA, TOTIMP1, TOTIMP2, TOTIMP3, TOTIMP4,
                    DESC1, DESC2, DESC3, COMI, ACT_INV, NUM_ALM, POLIT_APLI, TIP_CAM, UNI_VENTA,
                    TIPO_PROD, CVE_OBS, REG_SERIE, E_LTPD, TIPO_ELEM, TOT_PARTIDA, TOTIMP8, TOTIMP7,
                    TOTIMP6, TOTIMP5)
                VALUES(?,?,?,?,?,?,?,?,?,?,
                       ?,?,?,?,?,?,?,?,?,?,
                       ?,?,?,?,?,?,?,?,?,?,
                       ?,?,?,?,?,?,?,?)'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in list_parfactc_df:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[1] = int(row[1])
            row[7] = int(row[7])
            row[8] = int(row[8])
            row[9] = int(row[9])
            row[10] = int(row[10])
            row[11] = int(row[11])
            row[12] = int(row[12])
            row[13] = int(row[13])
            row[14] = int(row[14])
            row[15] = int(row[15])
            row[16] = float(row[16])
            row[17] = float(row[17])
            row[18] = float(row[18])
            row[19] = float(row[19])
            row[20] = int(row[20])
            row[21] = int(row[21])
            row[22] = int(row[22])
            row[24] = int(row[24])
            row[26] = int(row[26])
            row[29] = int(row[29])
            row[30] = int(row[30])
            row[31] = int(row[31])
            row[34] = int(row[34])
            row[35] = int(row[35])
            row[36] = int(row[36])
            row[37] = int(row[37])
            

            cursor.execute(sql, tuple(row))

        connection.commit()
        cursor.close()
        connection.close()
    

    except Exception as e:
        titulo = "Error en par_factc"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    
    return par_factc_df

def select_cvebita():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]

    sql_select_max = f'''SELECT 
                            MAX(CVE_BITA)
                        FROM {database_04}.dbo.BITA04'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_select_max)
        result_max = cursor.fetchall()
        result_max = [item for sublist in result_max for item in sublist]
        
        cve_bita = result_max[0]+1
                
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error existg"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

    return cve_bita

def general_factc(cve_doc,items_df, df_partidas, max_cvebita):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    total_impuestos = sum(df_partidas['totimp1'])+sum(df_partidas['totimp2'])+sum(df_partidas['totimp3'])+sum(df_partidas['totimp4'])

    cantidad_total = []
    for index, row in df_partidas.iterrows():
        precios = pd.to_numeric(row['precio'])*pd.to_numeric(row['cantidad'])
        cantidad_total.append(precios)
    cant_tot = sum(cantidad_total)
        
    
    gral_factc_df = pd.DataFrame({'tip_doc':['C'],'cve_doc':[cve_doc], 'cve_clpv':['WEB'], 'status':['O'],'cve_pedi':[items_df['documento']],
                                 'fecha_doc':[datetime.now().strftime("%Y-%m-%d")], 'fecha_ent':[datetime.now().strftime("%Y-%m-%d")], 'fecha_ven':[datetime.now().strftime("%Y-%m-%d")],
                                 'imptot1':[sum(df_partidas['totimp1'])], 'imptot2':[sum(df_partidas['totimp2'])], 'imptot3':[sum(df_partidas['totimp3'])], 'imptot4':[sum(df_partidas['totimp4'])],
                                 'des_fin':[0], 'com_tot':[0], 'num_moned':[1], 'tipcamb':[1],
                                 'primerpago':[0], 'rfc':[''], 'folio':[int(cve_doc)],
                                 'SERIE':['STAND.'], 'ESCFD':['N'], 'num_alma':[1],'act_cxc':['S'],
                                 'act_coi':['N'], 'can_tot':[sum(cantidad_total)],'cve_vend':[''], 'des_tot':[0],
                                 'num_pagos':[1], 'dat_envio':[0], 'contado':['N'], 
                                 'dat_mostr':[0], 'cve_bita':[max_cvebita], 'bloq':['N'], 'fecha_elab':[datetime.now().strftime("%Y-%m-%d  %H:%M:%S")],
                                 'ctlpol':[0], 'cve_obs':[0], 'enlazado':['O'],'tip_doc_e':['O'],
                                 'des_tot_porc':[0], 'importe':[total_impuestos+pd.to_numeric(sum(cantidad_total))], 'tip_fac':['C'], 'imp_tot8':[0],
                                 'imp_tot7':[0], 'imp_tot6':[0], 'imp_tot5':[0]})
    
    
    list_gral_factc = [tuple(x) for x in gral_factc_df.to_records(index=False)] 
    
    sql = f'''  INSERT INTO 
                    {database_04}.dbo.FACTC04
                    (TIP_DOC, CVE_DOC, CVE_CLPV, STATUS,
                    CVE_PEDI, FECHA_DOC, FECHA_ENT, FECHA_VEN,
                    IMP_TOT1, IMP_TOT2, IMP_TOT3, IMP_TOT4,
                    DES_FIN, COM_TOT, NUM_MONED, TIPCAMB,
                    PRIMERPAGO, RFC, FOLIO, SERIE,
                    ESCFD, NUM_ALMA, ACT_CXC, ACT_COI,
                    CAN_TOT, CVE_VEND, DES_TOT, NUM_PAGOS,
                    DAT_ENVIO, CONTADO, DAT_MOSTR, CVE_BITA,
                    BLOQ, FECHAELAB, CTLPOL, CVE_OBS,
                    ENLAZADO, TIP_DOC_E, DES_TOT_PORC, IMPORTE,
                    TIP_FAC, IMP_TOT8, IMP_TOT7, IMP_TOT6,IMP_TOT5)
                VALUES(?,?,?,?,
                       ?,?,?,?,
                       ?,?,?,?,
                       ?,?,?,?,
                       ?,?,?,?,
                       ?,?,?,?,
                       ?,?,?,?,
                       ?,?,?,?,
                       ?,?,?,?,
                       ?,?,?,?,
                       ?,?,?,?,?)'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in list_gral_factc:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[8] = float(row[8])
            row[9] = float(row[9])
            row[10] = float(row[10])
            row[11] = float(row[11])
            row[12] = int(row[12])
            row[13] = int(row[13])
            row[14] = int(row[14])
            row[15] = int(row[15])
            row[16] = int(row[16])

            row[18] = int(row[18])
            row[21] = int(row[21])
            row[26] = int(row[26])
            row[27] = int(row[27])
            row[28] = int(row[28])
            row[30] = int(row[30])
            row[31] = int(row[31])
            row[34] = int(row[34])
            row[35] = int(row[35])
            row[38] = int(row[38])

            row[41] = int(row[41])
            row[42] = int(row[42])
            row[43] = int(row[43])
            row[44] = int(row[44])
      
            

            cursor.execute(sql, tuple(row))

        connection.commit()
        cursor.close()
        connection.close()
    

    except Exception as e:
        titulo = "Error en gral_factc"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    
    return cant_tot, total_impuestos

def select_existg(cve_art):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    sql = f''' SELECT 
                    SUM(EXIST) AS TotalExistencias, 
                    SUM(CASE WHEN CVE_ALM = 1 THEN EXIST ELSE 0 END) AS ExistenciasAlmacen1 
                FROM {database_04}.dbo.MULT04
                WHERE CVE_ART = ? '''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, cve_art)
        result = cursor.fetchall()
        exist_result = [item for sublist in result for item in sublist]
        
        existg = exist_result[0]
        exist_01 = exist_result[1]
        
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error existg"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

    return existg, exist_01

def insert_bita(cve_bita, cve_doc, can_tot):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    actividad = ' ' * 3+'5'
    bita_df = pd.DataFrame({'cve_bita':[cve_bita], 'cve_clie':['WEB'], 'cve_campania':['_SAE_'],
                            'cve_actividad':[actividad], 'fechahora':[datetime.now().strftime("%Y-%m-%d %H:%M:%S")], 'cve_usuario':[177],
                            'observaciones':[f"No.[{cve_doc}] ${can_tot}"], 'status':['F'], 'nom_usuario':['foraneos']})

    list_bita = [tuple(x) for x in bita_df.to_records(index=False)] 

    sql = f'''INSERT INTO {database_04}.dbo.BITA04
                            (CVE_BITA, CVE_CLIE, CVE_CAMPANIA, CVE_ACTIVIDAD, FECHAHORA,
                             CVE_USUARIO, OBSERVACIONES, STATUS, NOM_USUARIO)
                            VALUES(?,?,?,?,?,?,?,?,?)'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        for row in list_bita:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[0] = int(row[0])
            row[3] = str(row[3])
            row[5] = int(row[5])


            cursor.execute(sql, tuple(row))

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error existg"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def afacta(can_tot, imp_tot):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    fecha_actual = datetime.now()
    mes = fecha_actual.month
    year = fecha_actual.year

    sql_select = f'''SELECT 
                CVE_AFACT
            FROM
                {database_04}.dbo.AFACT04
                WHERE
                    MONTH(PER_ACUM) = ?
                AND 
                    YEAR(PER_ACUM) = ?'''
    sql_update = f'''UPDATE
                        {database_04}.dbo.AFACT04
                    SET
                        CVTA_COM = CVTA_COM + ?,
                        CIMP = CIMP + ?
                    WHERE
                        CVE_AFACT = ?'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_select,(mes,year))
        result_select = cursor.fetchall()
        cve_afact = [item for sublist in result_select for item in sublist]
        
        cve_afact = cve_afact[0]

        cursor.execute(sql_update,(can_tot, imp_tot, cve_afact))
        connection.commit()

        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error existg"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def fact_clib(cve_doc):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    sql = f''' INSERT INTO FACTC_CLIB04
                    (CLAVE_DOC)
                VALUES
                    (?)'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        cursor.execute(sql, cve_doc)

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error fact_clib"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    
def parfactc_clib(cve_doc, num_partidas):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    sql = f''' INSERT INTO PAR_FACTC_CLIB04
                    (CLAVE_DOC, NUM_PART)
                VALUES
                    (?, ?)'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        cursor.execute(sql, (cve_doc, num_partidas))

        connection.commit()
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error campos libres factura (PAR_FACTC_CLIB)"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def update_tab_ctrl():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    sql_select = f''' SELECT
                        ULT_CVE
                FROM
                    TBLCONTROL04
                WHERE 
                    ID_TABLA = 32 OR ID_TABLA = 62'''
    
    sql_update_32 = f''' UPDATE
                        TBLCONTROL04
                    SET
                        ULT_CVE = ?
                    WHERE 
                        ID_TABLA = 32'''
    
    sql_update_62 = f''' UPDATE
                        TBLCONTROL04
                    SET
                        ULT_CVE = ?
                    WHERE 
                        ID_TABLA = 62'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        cursor.execute(sql_select)
        result_select = cursor.fetchall()
        list_ultcve = [item for sublist in result_select for item in sublist]

        ult_cve_32 = list_ultcve[0]+1
        ult_cve_62 = list_ultcve[1]+1

        cursor.execute(sql_update_32, ult_cve_32)
        connection.commit()

        cursor.execute(sql_update_62, ult_cve_62)
        connection.commit()

        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error fact_clib"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def update_folios_fac(cve_doc):
            # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    fecha = datetime.now().strftime("%Y-%m-%d")
    sql = f'''UPDATE 
                [{database_04}].[dbo].[FOLIOSF04]
            SET 
                ULT_DOC = ?,
                FECH_ULT_DOC = ?
            WHERE
                TIP_DOC = 'C'
            AND
                SERIE = 'STAND.'
            '''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (cve_doc, fecha))

        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error update folios_fac"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    

def update_pedweb(folio):
        # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    sql = f'''UPDATE
                        SlicTe.dbo.STe_PedWeb
                    SET
                        BAND = 'A'
                    WHERE
                        folio = ?'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, folio)

        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    

def web_movinve(pedido, max_num_mov, cpto_minve, fecha_docu, tipo_doc, existg, exist_01, unidad, fecha_elab, costeado):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    pedido['num_mov'] = max_num_mov
    pedido['cve_cpto'] = cpto_minve
    pedido['fecha_docu'] = fecha_docu
    pedido['tipo_doc'] = tipo_doc
    pedido['existg'] = existg
    pedido['exist_01'] = exist_01
    pedido['unidad'] = unidad
    pedido['fecha_elab'] = fecha_elab
    pedido['costeado'] = costeado
    pedido['vend'] = ""
    pedido['cant_cost'] = 0
    pedido['reg_serie'] = 0
    pedido['factor_con'] = 1
    pedido['signo'] = "-1"
    pedido["desde_inve"] = "N"
    pedido["mov_enlazado"] = 0
    pedido = pedido[["sku_vari", "almacen", "num_mov", "cve_cpto", "fecha_docu",
                     "tipo_doc", "documento", "cliente", "vend", "cantidad","cant_cost",
                      "precio", "costo", "reg_serie", "unidad", "existg","exist_01",
                      "factor_con", "fecha_elab", "signo","costeado", "costo",
                     "costo", "costo", "desde_inve", "mov_enlazado"]]
        
    
    sql_insert = f'''INSERT INTO  {database_04}.dbo.MINVE04
                        ([CVE_ART], [ALMACEN], [NUM_MOV], [CVE_CPTO], 
                        [FECHA_DOCU],[TIPO_DOC], [REFER], [CLAVE_CLPV], [VEND],
                        [CANT], [CANT_COST],[PRECIO],[COSTO], [REG_SERIE],
                        [UNI_VENTA], [EXIST_G], [EXISTENCIA], [FACTOR_CON], [FECHAELAB], 
                        [SIGNO], [COSTEADO], [COSTO_PROM_INI], [COSTO_PROM_FIN], [COSTO_PROM_GRAL],[DESDE_INVE], [MOV_ENLAZADO])
                    VALUES (?, ?, ?, ?, 
                            CONVERT(datetime, ?, 121), ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, ?, ?,
                            ?, ?, CONVERT(datetime, ?, 121), ?,
                            ?, ?, ?, ?,
                            ?, ?)'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        x = list(pedido)
        cursor.execute(sql_insert,x)
        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def modif_exist_inve04(existg,cantidad, cve_art):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    cant_actual = existg-cantidad
    sql_update = f''' UPDATE {database_04}.dbo.INVE04 SET EXIST = ? WHERE CVE_ART = ?'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

    
        cursor.execute(sql_update, cant_actual, cve_art)
        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error modif_exist_inve"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def modif_exist_mult04(exist, cantidad, cve_art):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    cant_actual = exist-cantidad
    sql_update = f''' UPDATE {database_04}.dbo.MULT04 SET EXIST = ? WHERE CVE_ART = ? AND CVE_ALM = 1'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

    
        cursor.execute(sql_update, cant_actual, cve_art)
        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error modif_exist_mult04"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def create_respaldo_exitmp():
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    
    # Example query
    sql = f'''IF NOT EXISTS (SELECT * FROM sys.objects WHERE object_id = OBJECT_ID(N'dbo.STe_Respaldo') AND type in (N'U'))
            BEGIN
                CREATE TABLE dbo.STe_RespaldoExiTMP (
                    folio INT,
                    skuvari VARCHAR(10) NOT NULL,   
                    desvari VARCHAR(100) NOT NULL,  
                    cant FLOAT NOT NULL,            
                    exist FLOAT NOT NULL,           
                    stMin FLOAT NOT NULL)
            END; '''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error en query"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def criterio_exitmp():

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    lista_folios  = []
    sql = f'''Select folio
              From [SlicTe].[dbo].[STe_RespaldoExiTMP]
              Group by folio'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_folios = cursor.fetchall()
        lista_folios = [list(row) for row in lista_folios]
        cursor.close()
        connection.close()

        lista_folios  = pd.DataFrame(lista_folios, columns = ["folio"] )
      
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_folios
 
def respaldo_exitmp(folio):

        x = criterio_exitmp()
        y = int(folio)
        if (x['folio'] == y).any():
            pass
        else:
            connectors = [
                SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
                ),
                SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database=f"{database_04}"
                )]
            sql_respaldo = f'''INSERT INTO [SlicTe].[dbo].[STe_RespaldoExiTMP] (folio, skuvari, desvari, cant, exist, stMin)
                    SELECT 
                    P.Folio, 
                    P.skuvari, 
                    P.desvari, 
                    SUM(ISNULL(P.cant, 0)) AS cant, 
                    AVG(ISNULL(I.EXIST, 0)) AS exist, 
                    AVG(ISNULL(I.STOCK_MIN, 0)) AS stMin
                    FROM 
                    STe_Pedidos P 
                    LEFT JOIN 
                    {database_04}.dbo.MULT04 I 
                    ON 
                    P.skuvari COLLATE Modern_Spanish_CI_AS = I.CVE_ART
                    WHERE 
                    P.Folio = ? AND CVE_ALM = 1
                    GROUP BY 
                    P.Folio, P.skuvari, P.desvari'''
    
            try:
                first_connector = connectors[0]
                connection = first_connector.connect()
                cursor = connection.cursor()
                cursor.execute(sql_respaldo, folio)
                connection.commit()
                cursor.close()
                connection.close()
                
            except Exception as e:
                titulo = "Error"
                mensaje = f"Error: {str(e)}"
                messagebox.showerror(titulo, mensaje)

def query_ctrl_orden_produccion(folio):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    lista_OP = []
    # Example query
    sql_op= f'''SELECT 
                    [OP], 
                    [skuvari], 
                    SUM([cant]) AS total_cant, 
                    MIN([desvari]) AS desvari, 
                    MIN([lote]) AS lote, 
                    MIN([consumo]) AS consumo, 
                    MIN([Folio]) AS Folio, 
                    MIN([N_S]) AS N_S, 
                    MIN([receta]) AS receta, 
                    MIN([tipo]) AS tipo
                FROM [SlicTe].[dbo].[STe_OP]
                WHERE [OP] = ?
                GROUP BY [OP], [skuvari]
                ORDER BY [skuvari] ASC'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_op, folio)
        lista_OP = cursor.fetchall()
        lista_OP = [list(row) for row in lista_OP]


        df_op = pd.DataFrame(lista_OP, columns = ["OP", "skuvari", "total_cant",
                                                     "desvari", "lote", "consumo",
                                                     "Folio", "N_S", "receta", "tipo"])
     
        df_op["N_S"] = df_op["N_S"].str.strip()


        lista_OP =  [tuple(x) for x in df_op.to_records(index=False)]

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_OP

def insert_pendientes(folio):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    
    lista = query_ctrl_orden_produccion(folio)

    # Example query
    sql = f'''INSERT INTO  [SlicTe].[dbo].[STe_Pedidos_pendientes] 
                    ([Folio] ,[Origen] ,[nopedido] ,[skuvari] ,[desvari] ,[cant_surt] ,[cant_falta])
              VALUES(?, ?, ?, ?, ?, ?, ?)'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in lista:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[0] = int(row[0]) 
            cursor.execute(sql, tuple(row))

        connection.commit()
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error en query"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def select_usuario(user_num):

     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]
    
    # Example query with proper string formatting
    sql = f'''SELECT U_Niv 
             FROM [SlicTe].[dbo].[STe_Usuarios]
             WHERE U_Nom = ?'''
    nivel = None
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, user_num)
        result = cursor.fetchone()
        if result:
            nivel = result.U_Niv
        cursor.close()
        connection.close()
      

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return nivel

def query_control_ordenes():

     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]

    lista_falta = []
    # Example query
    sql = f'''SELECT [Folio] ,[skuvari] ,[desvari] ,[cant] ,[exist] ,[Ban] , [N_S]
              FROM [SlicTe].[dbo].[STe_Falta]'''

        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_falta = cursor.fetchall()
        lista_falta = [list(row) for row in lista_falta]
        
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_falta

def query_control_ordenes_folio(folio):

     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]

    lista_falta = []
    # Example query
    sql = f'''SELECT [Folio] ,[skuvari] ,[desvari] ,[cant] ,[exist] ,[Ban] , [N_S]
              FROM [SlicTe].[dbo].[STe_Falta]
              Where Folio = ?'''

        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, folio)
        lista_falta = cursor.fetchall()
        lista_falta = [list(row) for row in lista_falta]
        
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_falta

def producido(op, sku, des, lote, consumo, folio, st_prod, st_prod_nuevo):

     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]
    #ajustar lote y desvari no deben de estar aqui
    sql = f'''UPDATE [SlicTe].[dbo].[STe_OP]
              SET N_S = ?
              Where OP = ? AND skuvari = ? AND desvari = ? AND lote = ? AND consumo = ? AND N_S = ?'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (st_prod_nuevo, op,  sku, des, lote, consumo,  st_prod))
        connection.commit()
        
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)

def materia_prima(sku, cantidad):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database=f"{database_05}"
        )
    ]
    lista = []
    sql = f'''SELECT P.CVE_ART, P.CVE_PROD, I.DESCR, (P.CANTIDAD*?) CantNec, I.EXIST, I.STOCK_MIN
                From {database_05}.dbo.KITS05 P, {database_05}.dbo.INVE05 I
                Where P.CVE_ART = ? and P.CVE_PROD = I.CVE_ART'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (cantidad, sku))


        lista = cursor.fetchall()
        lista = [list(row) for row in lista]

        materia_prima = pd.DataFrame(lista, columns = ["CVE_ART", "CVE_PROD", "DESCR","CantNec", "EXIST", "STOCK_MIN"])
        materia_prima["cant_salidas"] = materia_prima["CantNec"]
        
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)
    return materia_prima

def surtir_mp(folio, sku, des, cant, exist, ban, Ns, Ns_nuevo):

     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]

    sql = f'''UPDATE [SlicTe].[dbo].[STe_Falta]
              SET N_S = ?
              Where Folio = ? AND skuvari = ? AND desvari = ? AND cant = ? AND exist = ? AND Ban = ? AND N_S = ?'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (Ns_nuevo, folio, sku, des, cant, exist, ban, Ns))
        connection.commit()
        
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)

def recetas():
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database=f"{database_05}"
        )
    ]
    lista = []
    sql = f'''SELECT DISTINCT [CVE_ART] FROM {database_05}.dbo.KITS05'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)

        # lista = cursor.fetchall()
        lista = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)
    return lista

def variables_inventarios_salidas():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
    # Example query
    sql_num_mov= f''' SELECT max(NUM_MOV)FROM {database_05}.dbo.MINVE05'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_num_mov)

        v_max = cursor.fetchone()[0]

        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    
    
    return v_max

def variables_salidad_inve(cve_prod):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
    # Example query
    sql_uni= f''' SELECT [UNI_MED] FROM {database_05}.dbo.INVE05 where CVE_ART = ? '''
    sql_costo = f'''SELECT [COSTO_PROM] FROM {database_05}.dbo.INVE05 where CVE_ART = ?'''


    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_uni, cve_prod)

        unidad = cursor.fetchone()[0]

        cursor.execute(sql_costo, cve_prod)
        costo = cursor.fetchone()[0]

        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    
    
    return unidad, costo

def salidas(materia_df, op):
    v_max_nummov = variables_inventarios_salidas() 
    v_max_nummov = v_max_nummov +1
    op = str(op)
    op = "OP00000"+ op
    #range should be = to the number of rows in materia prima_df
    salidas_df = pd.DataFrame(index = range(len(materia_df)), columns = ['CVE_ART', 'ALMACEN', 'NUM_MOV', 'CVE_CPTO', 'FECHA_DOCU', 
                                                            'TIPO_DOC', 'REFER', 'CLAVE_CLPV', 'VEND','CANT', 'CANT_COST', 
                                                            'PRECIO', 'COSTO', 'AFEC_COI', 'CVE_OBS', 'REG_SERIE', 'UNI_VENTA',
                                                            'E_LTPD', 'EXIST_G', 'EXISTENCIA', 'TIPO_PROD', 'FACTOR_CON', 
                                                            'FECHAELAB', 'CTLPOL', 'CVE_FOLIO','SIGNO', 'COSTEADO', 'COSTO_PROM_INI', 
                                                            'COSTO_PROM_FIN', 'COSTO_PROM_GRAL', 'DESDE_INVE', 'MOV_ENLAZADO'])
    # salidas_df = pd.DataFrame(columns = ['CVE_ART', 'ALMACEN', 'NUM_MOV', 'CVE_CPTO', 'FECHA_DOCU', 
    #                                                         'TIPO_DOC', 'REFER', 'CLAVE_CLPV', 'VEND','CANT', 'CANT_COST', 
    #                                                         'PRECIO', 'COSTO', 'AFEC_COI', 'CVE_OBS', 'REG_SERIE', 'UNI_VENTA',
    #                                                         'E_LTPD', 'EXIST_G', 'EXISTENCIA', 'TIPO_PROD', 'FACTOR_CON', 
    #                                                         'FECHAELAB', 'CTLPOL', 'CVE_FOLIO','SIGNO', 'COSTEADO', 'COSTO_PROM_INI', 
    #                                                         'COSTO_PROM_FIN', 'COSTO_PROM_GRAL', 'DESDE_INVE', 'MOV_ENLAZADO'])
    
    salidas_df ['CVE_ART'] = materia_df['CVE_PROD'].values
    salidas_df['ALMACEN'] = 1
    salidas_df['NUM_MOV'] = range(v_max_nummov, v_max_nummov + len(salidas_df))
    salidas_df['FECHA_DOCU'] = datetime.now().strftime("%Y-%m-%d")
    salidas_df['TIPO_DOC'] = "M"
    salidas_df['REFER'] = op
    salidas_df['CANT'] = materia_df["cant_salidas"]*-1
    salidas_df['EXIST_G'] = materia_df["EXIST"]-materia_df["cant_salidas"]
    salidas_df['EXISTENCIA'] = materia_df["EXIST"]-materia_df["cant_salidas"]
    salidas_df['CVE_CPTO'] = 53
    salidas_df['REG_SERIE'] = 0
    salidas_df['TIPO_PROD'] = "P"
    salidas_df['FACTOR_CON'] = 1
    salidas_df['FECHAELAB'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    salidas_df['CVE_FOLIO'] = 1
    salidas_df['SIGNO'] = 1
    salidas_df['COSTEADO'] = "S"
    salidas_df['DESDE_INVE'] = "S"
    salidas_df['MOV_ENLAZADO'] = 0
    salidas_df['UNI_VENTA'] = materia_df['unidad']
    salidas_df['COSTO'] = materia_df['costo']
    salidas_df['COSTO_PROM_INI'] = materia_df['costo']
    salidas_df['COSTO_PROM_FIN'] = materia_df['costo']
    salidas_df['COSTO_PROM_GRAL'] = materia_df['costo']

    salidas_df = salidas_df.reset_index(drop=True)

    return salidas_df

def insert_salidas(salidas_df):
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database=f"{database_05}"
        )
    ]

         #change data type
    salidas_df['CVE_ART'] = salidas_df['CVE_ART'].astype(object)
    salidas_df['FECHA_DOCU'] = pd.to_datetime(salidas_df['FECHA_DOCU'])
    salidas_df['FACTOR_CON'] = salidas_df['FACTOR_CON'].astype(float)
    salidas_df['FECHAELAB'] = pd.to_datetime(salidas_df['FECHAELAB'])
    salidas_df['CVE_FOLIO'] = salidas_df['CVE_FOLIO'].astype(object)


    salidas_df = salidas_df.dropna(axis = 1, how = "any")
 

    lista =  [tuple(x) for x in salidas_df.to_records(index=False)]

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
        # Example query
    sql = f'''INSERT INTO {database_05}.dbo.MINVE05 
             ([CVE_ART], [ALMACEN], [NUM_MOV], [CVE_CPTO], [FECHA_DOCU],
              [TIPO_DOC], [REFER], [CANT], [COSTO], [REG_SERIE],
              [UNI_VENTA], [EXIST_G], [EXISTENCIA],
              [TIPO_PROD], [FACTOR_CON], [FECHAELAB], [CVE_FOLIO],
              [SIGNO], [COSTEADO], [COSTO_PROM_INI], [COSTO_PROM_FIN], [COSTO_PROM_GRAL],
              [DESDE_INVE], [MOV_ENLAZADO])
             VALUES (?, ?, ?, ?, CONVERT(datetime, ?, 121), ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, CONVERT(datetime, ?, 121), ?, ?, ?, ?, ?, ?, ?, ?)'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in lista:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            #give correct type to the rows
            row[1] = int(row[1])
            row[2] = int(row[2])
            row[3] = int(row[3])
            row[4] = pd.Timestamp(row[4]).to_pydatetime()
            row[9] = int(row[9])
            row[15] = pd.Timestamp(row[15]).to_pydatetime()
            row[17] = int(row[17])
            row[23] = int(row[23])

            cursor.execute(sql, tuple(row))
            
        connection.commit()
    except Exception as e:
        titulo = "Error en query insert_procedure3"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    finally:
        cursor.close()
        connection.close()

def insert_exist_salidas(salidas_df):
    df = salidas_df[['EXISTENCIA', 'CVE_ART']]

    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database=f"{database_05}"
        )
    ]

    sql = f'''UPDATE {database_05}.dbo.INVE05 
          SET EXIST = ? 
          WHERE CVE_ART = ?'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
# Iterate through the DataFrame and execute the SQL update for each row
        for index, row in df.iterrows():
            cursor.execute(sql, row['EXISTENCIA'], row['CVE_ART'])
    # Commit the transaction
        connection.commit()

    # Close the cursor and connection
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error en query existencias salidas"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def update_ctrl_05():
    v_max = variables_inventarios_salidas()
    max_2 = v_max


    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
    # Example query
    sql = f''' UPDATE {database_05}.dbo.TBLCONTROL05
                SET ULT_CVE = CASE 
                WHEN ID_TABLA = 44 THEN {max_2}
              END
                WHERE ID_TABLA IN (44)'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(sql)
        connection.commit()  # Commit the transaction

    except Exception as e:
        # Handle exceptions (e.g., show error message)
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

    finally:
            # Close cursor and connection
        cursor.close()
        connection.close()

def variables_inventarios_entradas():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    # Example query
    sql_num_mov= f''' SELECT max(NUM_MOV)FROM {database_04}.dbo.MINVE04'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_num_mov)

        v_max = cursor.fetchone()[0]

        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    
    
    return v_max

def variables_entradas_inve(cve_art):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    # Example query
    sql_uni= f''' SELECT [UNI_MED] FROM {database_04}.dbo.INVE04 where CVE_ART = ? '''
    sql_costo = f'''SELECT [COSTO_PROM] FROM {database_04}.dbo.INVE04 where CVE_ART = ?'''


    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_uni, cve_art)

        unidad = cursor.fetchone()[0]

        cursor.execute(sql_costo, cve_art)
        costo = cursor.fetchone()[0]

        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    
    
    return unidad, costo

def entradas(materia_df, op, items_df):
    v_max_nummov = variables_inventarios_entradas() 
    v_max_nummov = v_max_nummov +1
    op = str(op)
    op = "OP00000"+ op
    #range should be = to the number of rows in materia prima_df
    entradas_df = pd.DataFrame(index = range(len(materia_df)), columns = ['CVE_ART', 'ALMACEN', 'NUM_MOV', 'CVE_CPTO', 'FECHA_DOCU', 
                                                            'TIPO_DOC', 'REFER', 'CLAVE_CLPV', 'VEND','CANT', 'CANT_COST', 
                                                            'PRECIO', 'COSTO', 'AFEC_COI', 'CVE_OBS', 'REG_SERIE', 'UNI_VENTA',
                                                            'E_LTPD', 'EXIST_G', 'EXISTENCIA', 'TIPO_PROD', 'FACTOR_CON', 
                                                            'FECHAELAB', 'CTLPOL', 'CVE_FOLIO','SIGNO', 'COSTEADO', 'COSTO_PROM_INI', 
                                                            'COSTO_PROM_FIN', 'COSTO_PROM_GRAL', 'DESDE_INVE', 'MOV_ENLAZADO'])
    
   
    
    entradas_df['CVE_ART'] = materia_df['CVE_ART']
    entradas_df['ALMACEN'] = 1
    
    
    entradas_df['FECHA_DOCU'] = datetime.now().strftime("%Y-%m-%d")
    entradas_df['TIPO_DOC'] = "M"
    entradas_df['REFER'] = op
    


    entradas_df['CVE_CPTO'] = 3
    entradas_df['REG_SERIE'] = 0
    entradas_df['TIPO_PROD'] = "P"
    entradas_df['FACTOR_CON'] = 1
    entradas_df['FECHAELAB'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    entradas_df['CVE_FOLIO'] = 1
    entradas_df['SIGNO'] = 1
    entradas_df['COSTEADO'] = "S"
    entradas_df['DESDE_INVE'] = "S"
    entradas_df['MOV_ENLAZADO'] = 0
    entradas_df['UNI_VENTA'] = "pz"
    entradas_df['COSTO'] = materia_df['costo_entrada']
    entradas_df['COSTO_PROM_INI'] = materia_df['costo_entrada']
    entradas_df['COSTO_PROM_FIN'] = materia_df['costo_entrada']
    entradas_df['COSTO_PROM_GRAL'] = materia_df['costo_entrada']

    

    entradas_df = entradas_df.drop_duplicates(subset=["CVE_ART"])
    #moverlo despues de eliminar los duplicados y revisar que se este añadiendo un valor nuevo a cada fila
    entradas_df['NUM_MOV'] = range(v_max_nummov, v_max_nummov + len(entradas_df))

      # Reset index of items_df to align with entradas_df
    items_df = items_df.reset_index(drop=True)
    entradas_df = entradas_df.reset_index(drop=True)

    entradas_df['CANT'] = items_df["CANT"]
    entradas_df['EXIST_G'] = items_df["CANT"]
    entradas_df['EXISTENCIA'] = items_df["CANT"]

    return entradas_df

def insert_entradas(entradas_df, exist_inve, current_exist):
# Define multiple instances for different databases
     
    #define los valores correctos de las existencias
    #existencia general - suma de las existencias de todos los almacenes
    entradas_df["EXIST_G"] = exist_inve

    # suma de las existencias previas en el almacen + la cantidad que se va a meter
    entradas_df['EXISTENCIA'] = entradas_df['EXISTENCIA'] + current_exist

 
         #change data type
    entradas_df['CVE_ART'] = entradas_df['CVE_ART'].astype(object)
    entradas_df['FECHA_DOCU'] = pd.to_datetime(entradas_df['FECHA_DOCU'])
    entradas_df['FACTOR_CON'] = entradas_df['FACTOR_CON'].astype(float)
    entradas_df['FECHAELAB'] = pd.to_datetime(entradas_df['FECHAELAB'])
    entradas_df['CVE_FOLIO'] = entradas_df['CVE_FOLIO'].astype(object)


    entradas_df = entradas_df.dropna(axis = 1, how = "any")

    lista =  [tuple(x) for x in entradas_df.to_records(index=False)]

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
        # Example query
    sql = f'''INSERT INTO {database_04}.dbo.MINVE04 
             ([CVE_ART], [ALMACEN], [NUM_MOV], [CVE_CPTO], 
             [FECHA_DOCU],[TIPO_DOC], [REFER], [CANT], 
             [COSTO], [REG_SERIE],[UNI_VENTA], [EXIST_G], 
             [EXISTENCIA],[TIPO_PROD], [FACTOR_CON], [FECHAELAB], 
             [CVE_FOLIO],[SIGNO], [COSTEADO], [COSTO_PROM_INI], 
             [COSTO_PROM_FIN], [COSTO_PROM_GRAL],[DESDE_INVE], [MOV_ENLAZADO])
             VALUES (?, ?, ?, ?, 
             CONVERT(datetime, ?, 121), ?, ?, ?,
            ?, ?, ?, ?,
            ?, ?, ?, CONVERT(datetime, ?, 121),
            ?, ?, ?, ?,
              ?, ?, ?, ?)'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in lista:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            #give correct type to the rows
            row[1] = int(row[1])
            row[2] = int(row[2])
            row[3] = int(row[3])
            row[4] = pd.Timestamp(row[4]).to_pydatetime()
            row[7] = int(row[7])
            row[9] = int(row[9])
            row[11] = int(row[11])
            row[12] = int(row[12])
            row[15] = pd.Timestamp(row[15]).to_pydatetime()
            row[17] = int(row[17])
            row[23] = int(row[23])

            cursor.execute(sql, tuple(row))
            
        connection.commit()
    except Exception as e:
        titulo = "Error en query insert_entradas"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    finally:
        cursor.close()
        connection.close()

def insert_exist_entradas(entradas_df, exist_inve):

    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database=f"{database_05}"
        )
    ]

    sql = f'''UPDATE {database_04}.dbo.INVE04 
          SET EXIST = ? 
          WHERE CVE_ART = ?'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
# Iterate through the DataFrame and execute the SQL update for each row
        for index, row in entradas_df.iterrows():
            cursor.execute(sql, (exist_inve, row['CVE_ART']))
    # Commit the transaction
        connection.commit()

    # Close the cursor and connection
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error en query existencias entradas"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def update_ctrl_04():
    v_max = calc_max_ltpd04()
    max_inve = variables_inventarios_entradas()
 
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    # Example query
    sql = f''' UPDATE {database_04}.dbo.TBLCONTROL04
                SET ULT_CVE = CASE 
                WHEN ID_TABLA = 44 THEN {max_inve}
                WHEN ID_TABLA = 48 THEN {v_max}
              END
                WHERE ID_TABLA IN (44, 48)'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        # Execute the query
        cursor.execute(sql)
        connection.commit()  # Commit the transaction

    except Exception as e:
        # Handle exceptions (e.g., show error message)
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

    finally:
            # Close cursor and connection
        cursor.close()
        connection.close()

def almacen_04(entradas_df):

    df = entradas_df[['EXISTENCIA', 'CVE_ART']]

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    
    sql_1 = f'''SELECT EXIST FROM  {database_04}.dbo.MULT04 WHERE CVE_ART = ? AND CVE_ALM = 1'''

    # Example query
    sql_2 = f'''UPDATE {database_04}.dbo.MULT04 
          SET EXIST = ? 
          WHERE CVE_ART = ? AND CVE_ALM = 1'''
    
    sql_3 = f'''SELECT sum(EXIST) FROM {database_04}.dbo.MULT04 WHERE CVE_ART = ?'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        for index, row in df.iterrows():
            cursor.execute(sql_1, row["CVE_ART"])
            result = cursor.fetchone()
            if result:
                #existencias actuales
                current_exist = result[0]

                #actualiza las existencias 
                updated_exist = row["EXISTENCIA"] + current_exist
                cursor.execute(sql_2, updated_exist, row['CVE_ART'])
                connection.commit()

                #valor de las nuevas existencias
                cursor.execute(sql_3, row['CVE_ART'])
                result_2 = cursor.fetchone()
                if result_2:
                    exist_inve = result_2[0]
            else:
                current_exist = 0
                updated_exist = row["EXISTENCIA"] + current_exist
                exist_inve = updated_exist
        
    except Exception as e:
        titulo = "Error en query almacen_04"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

    finally:
            # Close cursor and connection
        cursor.close()
        connection.close()

    return exist_inve, current_exist

def ltpd_05(criterio_ltpd, criterio_ltpd_df):

    cve_art_mp = criterio_ltpd
    cve_art_df = criterio_ltpd_df

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}",
        )]
    
    lista_ltpd_data = []
        # para la materia prima
    sql_ltpd_data = f'''SELECT [CVE_ART] ,[LOTE] ,[PEDIMENTO] ,[CVE_ALM],[FCHCADUC],[FCHADUANA]
                ,[FCHULTMOV],[NOM_ADUAN],[CANTIDAD],[REG_LTPD],[CVE_OBS],[CIUDAD],[FRONTERA],[FEC_PROD_LT],[GLN]
                ,[STATUS],[PEDIMENTOSAT]
                FROM {database_05}.dbo.LTPD05
                where CVE_ART = ? AND CANTIDAD > 0''' #order by REG_LTPD DESC
# #
    try:
        #ltpd materia prima
        first_connector = connectors[0]

        connection = first_connector.connect()
        cursor = connection.cursor()
        for i in cve_art_mp:
            cursor.execute(sql_ltpd_data, i)
            row = cursor.fetchall()
            if row:
                lista_ltpd_data.extend(row)
        lista_ltpd_data = [list(row) for row in lista_ltpd_data]

        cursor.close()
        connection.close()

        len_list_05 = len(lista_ltpd_data)

        # lista_ltpd_data = pd.DataFrame(lista_ltpd_data)
        # print(lista_ltpd_data)
        if len_list_05 > 0:
############################################################################################
            ltpd05_data = pd.DataFrame(lista_ltpd_data, columns = ["CVE_ART", "LOTE", "PEDIMENTO", "CVE_ALM", 
                                                                      "FCHCADUC", "FCHADUANA", "FCHULTMOV",
                                                                      "NOM_ADUAN", "CANTIDAD", "REG_LTPD",
                                                                      "CVE_OBS", "CIUDAD", "FRONTERA",
                                                                      "FEC_PROD_LT", "GLN", "STATUS", "PEDIMENTOSAT"])
        
            if not ltpd05_data.index.equals(cve_art_df.index):
                ltpd05_data.reset_index(drop=True, inplace=True)
                cve_art_df.reset_index(drop=True, inplace=True)

            cant_mp = cve_art_df['CANT'].iloc[0]
            ltpd05_data['comparasion'] = ""
            cant_mp_abs = cant_mp*-1
            
            # Iterate over each row in the DataFrame
            for index, row in ltpd05_data.iterrows():
                if row['CANTIDAD'] >= cant_mp_abs:
                    ltpd05_data.at[index, "CANTIDAD"] += cant_mp
                    ltpd05_data.at[index, 'comparasion'] = 'mayor'
                    break
                else:
                    ltpd05_data.at[index, 'comparasion'] = 'menor'
                    ltpd05_data.at[index, "CANTIDAD"] += cant_mp

            ltpd05_data = ltpd05_data[ltpd05_data['comparasion'] != ""]    
            # print(ltpd05_data[["CVE_ART","CANTIDAD", "comparasion"]])
#             ltpd05_data["CANTIDAD"] += cant_mp
# #SI LA CANTIDAD ANTERIOR ES MAYOR O IGUAL QUE LA ACTUAL NO HAGAS TOD EL PROCESO PARA LOS DOS RENGLONES
            for i, row in ltpd05_data.iterrows():
                if ltpd05_data.at[i, "CANTIDAD"] < 0:
                    dif = ltpd05_data.at[i,"CANTIDAD"] - cant_mp
                    if i + 1 < len(ltpd05_data):
                        ltpd05_data.at[i+1, "CANTIDAD"] += dif
                    ltpd05_data.at[i, "CANTIDAD"] = 0
                else:
                    dif = 0
            
            ltpd05_data["FCHULTMOV"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

            data_ltpd05 = ltpd05_data[["FCHULTMOV", "CANTIDAD", "REG_LTPD"]]
            #obtener el valor del pedimento
            max_index = ltpd05_data["CANTIDAD"].idxmax()
            pedimento =  ltpd05_data.at[max_index, 'PEDIMENTO']
# # # ########################################################################
#insert ltpd 05
            # ltpd05_data = ltpd05_data.dropna(axis = 1, how = "any")
            lista_05 =  [tuple(x) for x in data_ltpd05.to_records(index=False)]

            # #   # Example query
            sql_update_05 = f'''UPDATE Empre05SQL.dbo.LTPD05
                                SET FCHULTMOV = ?, CANTIDAD = ? 
                                WHERE REG_LTPD = ?'''
            first_connector = connectors[0]
            connection = first_connector.connect()
            cursor = connection.cursor()

            for row in lista_05:
            # Ensure the row is a list and convert the fields accordingly
                row = list(row)
                #give correct type to the rows
                row[2] = int(row[2])

                cursor.execute(sql_update_05, tuple(row))
            
            connection.commit()
# # #######################################################################

#             valid = 1
        else:
            pedimento = " "
            messagebox.showwarning("AVISO!", "no hay pedimentos de materia prima para" + cve_art_mp[0])

    except Exception as e:
        titulo = "Error ltpd05"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)
    return pedimento

def ltpd_04(entradas_df, lote):
    
    cve_art_te_df = entradas_df.drop_duplicates(subset=["producto"])
    cve_art_te = cve_art_te_df['producto'].tolist()
    pedimento = entradas_df["pedimento"].tolist()

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}",
        )]
    
    #para las ventas

    lista_ltpd_data_04 = []
        # Example query
    sql_ltpd_data_04 = f'''SELECT TOP (1) [CVE_ART] ,[LOTE] ,[PEDIMENTO] ,[CVE_ALM],[FCHCADUC],[FCHADUANA]
                ,[FCHULTMOV],[NOM_ADUAN],[CANTIDAD],[REG_LTPD],[CVE_OBS],[CIUDAD],[FRONTERA],[FEC_PROD_LT],[GLN]
                ,[STATUS],[PEDIMENTOSAT]
                FROM {database_04}.dbo.LTPD04
                where CVE_ART = ?
                order by REG_LTPD desc'''
    sql_max_reg_04 = f'''SELECT MAX(REG_LTPD) FROM {database_04}.dbo.LTPD04'''
    
    try:
        #ltpd 04
        first_connector = connectors[0]

        connection = first_connector.connect()
        cursor = connection.cursor()
        for j in cve_art_te:
            cursor.execute(sql_ltpd_data_04, j)
            row = cursor.fetchone()
            if row:
                lista_ltpd_data_04.append(row)
        lista_ltpd_data_04 = [list(row) for row in lista_ltpd_data_04]

         #max reg 04
        # first_connector = connectors[0]
        # connection = first_connector.connect()
        # cursor = connection.cursor()
        cursor.execute(sql_max_reg_04)

        max_reg_04 = cursor.fetchone()[0]
        # cursor.close()
        # connection.close()
        print('si')
#######################################################
        ltpd_data_04 = pd.DataFrame(lista_ltpd_data_04, columns = ["CVE_ART", "LOTE", "PEDIMENTO", "CVE_ALM", 
                                                              "FCHCADUC", "FCHADUANA", "FCHULTMOV",
                                                              "NOM_ADUAN", "CANTIDAD", "REG_LTPD",
                                                              "CVE_OBS", "CIUDAD", "FRONTERA",
                                                              "FEC_PROD_LT", "GLN", "STATUS", "PEDIMENTOSAT"])

               
        if not ltpd_data_04.index.equals(cve_art_te_df.index):
            ltpd_data_04.reset_index(drop=True, inplace=True)
            cve_art_te_df.reset_index(drop=True, inplace=True)

        
        ltpd_data_04["CANTIDAD"] = cve_art_te_df["CANT"]
        

        num_rows = len(ltpd_data_04)
        max_reg_04 = max_reg_04+1
        

        ltpd_data_04["REG_LTPD"] = range(max_reg_04, max_reg_04 + num_rows)
        ltpd_data_04["CVE_OBS"] = None
        ltpd_data_04["LOTE"] = lote
        ltpd_data_04["GLN"] = None
        ltpd_data_04["PEDIMENTO"] = pedimento
        ltpd_data_04["FCHULTMOV"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
        ltpd_data_04["FEC_PROD_LT"] = datetime.now().strftime("%Y-%m-%d")
        ltpd_data_04['CVE_ALM'] = 1
        len_lista_04 = len(lista_ltpd_data_04)
        max_ctrl = max(ltpd_data_04["REG_LTPD"])
        
        if len_lista_04 > 0:
#######################################################################
# insert ltpd 04
            ltpd_data_04 = ltpd_data_04.dropna(axis = 1, how = "any")
            if 'FCHADUANA' not in ltpd_data_04.columns:
                ltpd_data_04.insert(5, 'FCHADUANA', '1900-01-01 00:00:00' )

            lista_04 =  [tuple(x) for x in ltpd_data_04.to_records(index=False)]
  
# Example query
            sql_insert_04 = f'''INSERT INTO {database_04}.dbo.LTPD04 
                ([CVE_ART],[LOTE],[PEDIMENTO],[CVE_ALM],[FCHCADUC],[FCHADUANA],[FCHULTMOV],[NOM_ADUAN],[CANTIDAD],[REG_LTPD]
                ,[CIUDAD],[FRONTERA],[FEC_PROD_LT],[STATUS],[PEDIMENTOSAT])
                VALUES (?, ?, ?, ?, CONVERT(datetime, ?, 121), CONVERT(datetime, ?, 121), CONVERT(datetime, ?, 121), ?, ?, ?, ?, ?,CONVERT(datetime, ?, 121),?,?)'''
            print(ltpd_data_04)
            # first_connector = connectors[0]
            # connection = first_connector.connect()
            # cursor = connection.cursor()
            for k in lista_04:
            # Ensure the row is a list and convert the fields accordingly
                k = list(k)

            #give correct type to the rows

               # Give correct type to the rows
    # Give correct type to the rows
                k[1] = int(k[1])
                k[3] = int(k[3])     
                k[4] = pd.Timestamp(k[4]).to_pydatetime()
                k[5] = pd.Timestamp(k[5]).to_pydatetime()
                k[8] = int(k[8])
                k[9] = int(k[9])
                k[12] = pd.Timestamp(k[12]).to_pydatetime()
     

                cursor.execute(sql_insert_04, tuple(k))

            
                connection.commit()
            cursor.close()
            messagebox.showinfo('aaaaa', 'aaaaaaaaaaa')
        else:
            messagebox.showerror("Error en pedimentos", "No existe pedimento para algun artículo seleccionado")
            max_ctrl = 0

    except Exception as e:
        titulo = "Error AAAAAAAAAAAAAAA"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)
    return  max_ctrl

def consumo_nuevo(op, skuvari,consumo_nuevo):

     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]

    sql = f'''UPDATE [SlicTe].[dbo].[STe_OP]
              SET consumo = ?
              Where OP = ? AND skuvari = ?'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (consumo_nuevo, op, skuvari))
        connection.commit()
        
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)

def agregar_productos_pp(lista_productos):

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    
        # Example query

    sql = f'''INSERT INTO SlicTe.dbo.STe_ProdPro
             ([lote], [consumo], [sku],[cantidad], [desvari])
             VALUES (?, ?, ?, ?, ?)'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in lista_productos:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            cursor.execute(sql, tuple(row))

        connection.commit()
    except Exception as e:
        titulo = "Error en query agregar productos"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    finally:
        cursor.close()
        connection.close()

def des_product(cve_art):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}"
        )]
    sql = f'''SELECT I.DESCR AS desvari
                FROM {database_04}.dbo.INVE04 I
				WHERE CVE_ART = ?'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, cve_art)

        desvari = cursor.fetchone()[0]
    
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

    return desvari
        
def listar_pp():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    
        # Example query
    sql = f'''Select * From SlicTe.dbo.STe_ProdPro'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        cursor.execute(sql)
        lista_pp = cursor.fetchall()
        lista_pp = [list(row) for row in lista_pp]
        cursor.close()
        connection.close()
        

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_pp

def seleccionar_pp():
    recetas_df = recetas()
    # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    lista_pp = []
    # Example query
    sql = f'''SELECT P.lote, P.consumo, P.sku, P.cantidad, P.desvari
                FROM SlicTe.dbo.STe_ProdPro P '''
    sql_max = f'''SELECT isnull(max(op),0) from SlicTe.dbo.STe_OP'''
    sql_insert = f'''INSERT INTO SlicTe.dbo.STe_OP
                    ([lote],[consumo],[skuvari],[cant]
                    ,[desvari],[OP],[tipo],
                    [receta],[N_S])
                    VALUES (?,?,?,?,
                            ?,?,?,?,
                            ?)'''
    sql_delete = f'''Delete FROM SlicTe.dbo.STe_ProdPro'''

        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_pp = cursor.fetchall()
        lista_pp = [list(row) for row in lista_pp]

        cursor.execute(sql_max)
        max_op = cursor.fetchone()[0]

        

        pp = pd.DataFrame(lista_pp, columns=["lote","consumo", "sku", "cantidad", "desvari"])
        pp["OP"] = max_op+1
        pp["tipo"] = "Programada"
        

        #run the function recetas to know all the sku that have a recipe
        receta = recetas()
    #create an empty list            
        receta_df = []

    #iterate over each row of the df in the column skuvari
        for index, row in pp.iterrows():
            if row['sku'] in receta:
                receta_df.append('CON_RECETA')
            else:
                receta_df.append('SIN_RECETA')
    #add the results of the previous loop to the df
        pp["Receta"] = receta_df
        pp["N_S"] = "Por_producir"

        # file_name = resource_path(f"client/Programada_00{max_op+1}.csv")
        # pp.to_csv(file_name)
        
    #inserta el df de pp en la tabla OP

        lista_pp =  [tuple(x) for x in pp.to_records(index=False)]

        for row in lista_pp:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[3] = int(row[3])
            row[5] = int(row[5])  

            cursor.execute(sql_insert, tuple(row))
            connection.commit()
    
        cursor.execute(sql_delete)
        connection.commit()
        
        cursor.close()
        connection.close()
        pp.columns = ['Lote', 'Consumo', 'SKU', 'Cantidad',
                      'Descripcion', 'OP', 'Tipo', 'Receta', 'Estado']
        file_name = resource_path(f"client/Programada_00{max_op+1}.csv")
        pp.to_csv(file_name)


        messagebox.showinfo("Éxito!", f"Se agregaron artículos a nueva OP programada # {max_op + 1}" )


    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)

def eliminar_pp(sku_pp):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
     
    sql_delete = f'''Delete FROM SlicTe.dbo.STe_ProdPro
                    WHERE sku = ?'''

        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_delete, sku_pp)

        connection.commit()

        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Eliminar de registro"
        mensaje = f"No se pudo eliminar la información: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    
def select_lista_pedimentos():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}")]
    
        # Example query
    sql = f'''SELECT DISTINCT 
                    K.CVE_PROD
                FROM {database_05}.dbo.KITS05 K
                LEFT JOIN {database_05}.dbo.INVE05 I ON K.CVE_PROD = I.CVE_ART
                WHERE 
                    K.CVE_PROD LIKE 'ETB%' OR
                    K.CVE_PROD LIKE 'MLVT%' OR
                    K.CVE_PROD LIKE 'TGL%' OR
                    K.CVE_PROD LIKE 'X%' AND
                    K.CANTIDAD > 1
                ORDER BY 
                    K.CVE_PROD ASC'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        cursor.execute(sql)
        lista_mp_pedimentos = cursor.fetchall()
        lista_mp_pedimentos = [list(row) for row in lista_mp_pedimentos]
        cursor.close()
        connection.close()

        mp_pedimentos = pd.DataFrame(lista_mp_pedimentos, columns = ["materias"])
        

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return mp_pedimentos
    
def pedi_05(criterio):

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}",
        )]
    
    lista_pedi = []
        # para la materia prima
    sql_ltpd_data = f'''SELECT [CVE_ART] ,[LOTE] ,[PEDIMENTO] ,[CVE_ALM],[FCHCADUC],[FCHADUANA]
                ,[FCHULTMOV],[NOM_ADUAN],[CANTIDAD],[REG_LTPD],[CVE_OBS],[CIUDAD],[FRONTERA],[FEC_PROD_LT],[GLN]
                ,[STATUS],[PEDIMENTOSAT]
                FROM {database_05}.dbo.LTPD05
                where CVE_ART = ? AND CANTIDAD > 0''' #order by REG_LTPD DESC
# #
    try:
        #ltpd materia prima
        first_connector = connectors[0]

        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_ltpd_data, criterio)
        lista_pedi = cursor.fetchall()
        lista_pedi = [list(row) for row in lista_pedi]
        cursor.close()
        connection.close()

        len_list_05 = len(lista_pedi)

        if len_list_05 > 0:
############################################################################################
            ltpd05_data = pd.DataFrame(lista_pedi, columns = ["CVE_ART", "LOTE", "PEDIMENTO", "CVE_ALM", 
                                                                      "FCHCADUC", "FCHADUANA", "FCHULTMOV",
                                                                      "NOM_ADUAN", "CANTIDAD", "REG_LTPD",
                                                                      "CVE_OBS", "CIUDAD", "FRONTERA",
                                                                      "FEC_PROD_LT", "GLN", "STATUS", "PEDIMENTOSAT"])
        else:
            ltpd05_data = pd.DataFrame(index = [0], columns = ["CVE_ART", "LOTE", "PEDIMENTO", "CVE_ALM", 
                                                                      "FCHCADUC", "FCHADUANA", "FCHULTMOV",
                                                                      "NOM_ADUAN", "CANTIDAD", "REG_LTPD",
                                                                      "CVE_OBS", "CIUDAD", "FRONTERA",
                                                                      "FEC_PROD_LT", "GLN", "STATUS", "PEDIMENTOSAT"])
            ltpd05_data["CVE_ART"] = criterio
            ltpd05_data["PEDIMENTO"] = "SIN PEDIMENTO"
            
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return ltpd05_data

def update_ltpd05(data_ltpd05):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}",
        )]
#insert ltpd 05
    # ltpd05_data = ltpd05_data.dropna(axis = 1, how = "any")
    lista_05 =  [tuple(x) for x in data_ltpd05.to_records(index=False)]

    # #   # Example query
    sql_update_05 = f'''UPDATE {database_05}.dbo.LTPD05
                        SET FCHULTMOV = ?, CANTIDAD = ? 
                        WHERE REG_LTPD = ?'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        for row in lista_05:
            # Ensure the row is a list and convert the fields accordingly
                row = list(row)
                #give correct type to the rows
                row[2] = int(row[2])

                cursor.execute(sql_update_05, tuple(row))
            
        connection.commit()

        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error ltpd05"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)

def buscar_pedi_04(articulo):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}",
        )]
    
    lista_pedi = []
        # para la materia prima
    sql_ltpd_data = f'''SELECT TOP (1)[CVE_ART] ,[LOTE] ,[PEDIMENTO] ,[CVE_ALM],[FCHCADUC],[FCHADUANA]
                ,[FCHULTMOV],[NOM_ADUAN],[CANTIDAD],[REG_LTPD],[CVE_OBS],[CIUDAD],[FRONTERA],[FEC_PROD_LT],[GLN]
                ,[STATUS],[PEDIMENTOSAT]
                FROM {database_04}.dbo.LTPD04
                where CVE_ART = ? 
                ORDER BY REG_LTPD DESC''' #order by REG_LTPD DESC
# #
    try:
        #ltpd materia prima
        first_connector = connectors[0]

        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_ltpd_data, articulo)
        lista_pedi = cursor.fetchall()
        lista_pedi = [list(row) for row in lista_pedi]
        cursor.close()
        connection.close()

        len_list_04 = len(lista_pedi)

        if len_list_04 > 0:
############################################################################################
            ltpd04_data = pd.DataFrame(lista_pedi, columns = ["CVE_ART", "LOTE", "PEDIMENTO", "CVE_ALM", 
                                                                      "FCHCADUC", "FCHADUANA", "FCHULTMOV",
                                                                      "NOM_ADUAN", "CANTIDAD", "REG_LTPD",
                                                                      "CVE_OBS", "CIUDAD", "FRONTERA",
                                                                      "FEC_PROD_LT", "GLN", "STATUS", "PEDIMENTOSAT"])
        else:
            ltpd04_data = pd.DataFrame(index = [0], columns = ["CVE_ART", "LOTE", "PEDIMENTO", "CVE_ALM", 
                                                                      "FCHCADUC", "FCHADUANA", "FCHULTMOV",
                                                                      "NOM_ADUAN", "CANTIDAD", "REG_LTPD",
                                                                      "CVE_OBS", "CIUDAD", "FRONTERA",
                                                                      "FEC_PROD_LT", "GLN", "STATUS", "PEDIMENTOSAT"])
            ltpd04_data["CVE_ART"] = articulo
            ltpd04_data["PEDIMENTO"] = "SIN PEDIMENTO"
            
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return ltpd04_data

def calc_max_ltpd04():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}",
        )]
    
        # para la materia prima
    sql_ltpd_data = f'''SELECT MAX(REG_LTPD)FROM {database_04}.dbo.LTPD04''' 
# #
    try:
        #ltpd materia prima
        first_connector = connectors[0]

        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_ltpd_data)
        max_04 = cursor.fetchone()[0]
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)
    return max_04

def insert_ltpd04(lista_04):
    lista_04_df = pd.DataFrame(lista_04, columns = ["producto", "LOTE_y", "PEDIMENTO", "CVE_ALM", "CONSUMO", "FCHADUANA",
                                        "FCHULTMOV", 'NOM_ADUAN', 'CANT_PT', 'REG_LTPD',
                                        'CVE_OBS', 'CIUDAD', "FRONTERA", "FEC_PROD_LT", 'GLN',
                                        'STATUS', 'PEDIMENTOSAT'])

    serie_regltpd = pd.Series(range(max(lista_04_df['REG_LTPD']), max(lista_04_df['REG_LTPD']) + len(lista_04_df)))
    lista_04_df['REG_LTPD'] = serie_regltpd
    
    lista_04 = lista_04_df.values.tolist()

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_04}",
        )]
    
        # para la materia prima
    sql = f'''INSERT INTO {database_04}.dbo.LTPD04
            ([CVE_ART],[LOTE],[PEDIMENTO],[CVE_ALM],[FCHCADUC],
		    [FCHADUANA],[FCHULTMOV],[NOM_ADUAN],[CANTIDAD],[REG_LTPD],
		    [CVE_OBS],[CIUDAD],[FRONTERA],[FEC_PROD_LT],[GLN],
		    [STATUS],[PEDIMENTOSAT])
            VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for k in lista_04:
            # Ensure the row is a list and convert the fields accordingly
            k = list(k)
            k[4] = datetime.strptime(k[4], '%d-%m-%Y')
            # k[4] = pd.Timestamp(k[4]).to_pydatetime()  # FCHCADUC
            # k[5] = pd.Timestamp(k[5]).to_pydatetime()  # FCHADUANA
            # k[6] = pd.Timestamp(k[6]).to_pydatetime()  # FCHULTMOV
            # k[13] = pd.Timestamp(k[13]).to_pydatetime()  # FEC_PROD_LT
            cursor.execute(sql, tuple(k))

            connection.commit()
        cursor.close()
    except Exception as e:
        titulo = f"Error insert ltpd04 {lista_04}"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)

def modificar_lote_nuevo(op, skuvari, lote_nuevo):

     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]

    sql = f'''UPDATE [SlicTe].[dbo].[STe_OP]
              SET lote = ?
              Where OP = ? AND skuvari = ?'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (lote_nuevo, op, skuvari))
        connection.commit()
        
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)

def convertir_fecha(fecha):
    fecha_dt = datetime.strptime(fecha, "%m/%Y")
    return fecha_dt.strftime("01-%m-%Y")

def query_clave_admin():
    result = []
        # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    # Example query
    sql = f'''SELECT [U_Nom],
                     [U_Pas]
               FROM 
                    [SlicTe].[dbo].[STe_Usuarios]
                WHERE
                    [U_Nom] = 'Admin' '''
        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        result = cursor.fetchall()
        result = [list(row) for row in result]
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    return result

def listar_users():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    
        # Example query
    sql = f'''SELECT [U_Num],[U_Nom],[U_Niv] 
                FROM [SlicTe].[dbo].[STe_Usuarios]
                where U_Nom NOT LIKE 'Admin' '''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()

        cursor.execute(sql)
        lista_users = cursor.fetchall()
        lista_users = [list(row) for row in lista_users]
        cursor.close()
        connection.close()
        

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_users

def eliminar_user(numero_usuario):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
     
    sql_delete = f'''Delete FROM SlicTe.dbo.STe_Usuarios
                    WHERE U_num = ?'''

        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_delete, numero_usuario)

        connection.commit()

        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Eliminar de registro"
        mensaje = f"No se pudo eliminar la información: {str(e)}"
        messagebox.showerror(titulo, mensaje)

def list_of_names():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    
    lista = []
    sql = f'''SELECT DISTINCT [U_Nom] FROM [SlicTe].[dbo].[STe_Usuarios]'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)

        # lista = cursor.fetchall()
        lista = [row[0] for row in cursor.fetchall()]
        
        cursor.close()
        connection.close()

        stripped_list = [item.strip() for item in lista]

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)
    return stripped_list

def guardar(lista, num_usuario):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    nivel = lista[0]
    nombre = lista[1]
    clave = lista[2]
    
    sql = f'''UPDATE SlicTe.dbo.STe_Usuarios
             SET U_Niv = ?, U_Nom = ?, U_Pas = ?
             WHERE U_Num = ?'''

        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (nivel, nombre, clave, num_usuario))

        connection.commit()

        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "ERROR!"
        mensaje = f"No se pudo actualizar la información: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def query_ctrl_pt_falta(folio):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    lista_compra = []
    # Example query
    sql= f'''SELECT [folio], [skuvari], [desvari], [cantidad], [N_S]
                FROM [SlicTe].[dbo].[STe_PorEntrega]
                WHERE [Folio] = ?
                ORDER BY [skuvari] ASC'''
    
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, folio)
        lista_compra = cursor.fetchall()
        lista_compra = [list(row) for row in lista_compra]

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return lista_compra

def max_folio_requi():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
    # Example query
    sql = f''' SELECT max(CVE_DOC)FROM {database_05}.dbo.COMPQ05'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)

        v_max = cursor.fetchone()[0]
        v_max = int(v_max)

        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    
    return v_max

def requi_compra(folio):
    max_cve_doc = max_folio_requi()
    max_cve_doc = max_cve_doc+1
    compq_df = pd.DataFrame(columns=["TIP_DOC", "CVE_DOC", "STATUS", "SU_REFER", "FECHA_DOC",
                                     "FECHA_REC", "FECHA_PAG", "CANT_TOT", "IMP_TOT1", "IMP_TOT2",
                                     "IMP_TOT3", "IMP_TOT4", "DES_TOT", "DES_FIN", "TOT_IND",
                                     "OBS_COND", "CVE_OBS", "NUM_ALMA", "ACT_CXP", "ACT_COI",
                                     "ENLAZADO", "TIP_DOC_E", "NUM_MONED", "TIPCAMB", "FECHAELAB",
                                     "SERIE", "FOLIO", "CTLPOL", "ESCFD", "CONTADO",
                                     "BLOQ", "DES_FIN_PORC", "DES_TOT_PORC", "IMPORTE", "TIP_DOC_ANT", "DOC_ANT"])
    data = {
        "TIP_DOC": "q",
        "CVE_DOC": " " * 10 + str(max_cve_doc).zfill(10),
        "STATUS": "O",
        "SU_REFER": "",
        "FECHA_DOC": datetime.now().strftime("%Y-%m-%d"),
        "FECHA_REC": datetime.now().strftime("%Y-%m-%d"),
        "FECHA_PAG": datetime.now().strftime("%Y-%m-%d"),
        "CANT_TOT": 0,
        "IMP_TOT1": 0,
        "IMP_TOT2": 0,
        "IMP_TOT3": 0,
        "IMP_TOT4": 0,
        "DES_TOT": 0,
        "DES_FIN": 0,
        "TOT_IND": 0,
        "OBS_COND": f"folio pedidos #{folio}",
        "CVE_OBS": 0,  # Agregar la clave de observaciones desde antes
        "NUM_ALMA": 1,
        "ACT_CXP": "S",
        "ACT_COI": "N",
        "ENLAZADO": "O",
        "TIP_DOC_E": "O",
        "NUM_MONED": 1,
        "TIPCAMB": 1,
        "FECHAELAB": datetime.now().strftime("%Y-%m-%d %H:%M:%S"),
        "SERIE": "",
        "FOLIO": max_cve_doc,
        "CTLPOL": 0,
        "ESCFD": "N",
        "CONTADO": "N",
        "BLOQ": "N",
        "DES_FIN_PORC": 0,
        "DES_TOT_PORC": 0,
        "IMPORTE": 0,
        "TIP_DOC_ANT": "",
        "DOC_ANT": ""
    }
    compq_df.loc[len(compq_df)] = data

    lista =  [tuple(x) for x in compq_df.to_records(index=False)]
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
        # Example query
    sql = f'''INSERT INTO {database_05}.dbo.COMPQ05 
             ([TIP_DOC], [CVE_DOC], [STATUS], [SU_REFER], [FECHA_DOC],
             [FECHA_REC], [FECHA_PAG], [CAN_TOT], [IMP_TOT1], [IMP_TOT2],
             [IMP_TOT3], [IMP_TOT4], [DES_TOT], [DES_FIN], [TOT_IND],
             [OBS_COND], [CVE_OBS], [NUM_ALMA], [ACT_CXP], [ACT_COI],
             [ENLAZADO], [TIP_DOC_E], [NUM_MONED], [TIPCAMB], [FECHAELAB],
             [SERIE], [FOLIO], [CTLPOL], [ESCFD], [CONTADO],
             [BLOQ], [DES_FIN_PORC], [DES_TOT_PORC], [IMPORTE], [TIP_DOC_ANT], [DOC_ANT])
             VALUES (?, ?, ?, ?,CONVERT(datetime, ?, 121),
                     CONVERT(datetime, ?, 121), CONVERT(datetime, ?, 121), ?, ?, ?,
                     ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, CONVERT(datetime, ?, 121),
                     ?, ?, ?, ?, ?,
                     ?, ?, ?, ?, ?,?)'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in lista:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            #give correct type to the rows
            row[4] = pd.Timestamp(row[4]).to_pydatetime()
            row[5] = pd.Timestamp(row[5]).to_pydatetime()
            row[6] = pd.Timestamp(row[6]).to_pydatetime()
            row[7] = int(row[7])
            row[8] = int(row[8])
            row[9] = int(row[9])
            row[10] = int(row[10])
            row[11] = int(row[11])
            row[12] = int(row[12])
            row[13] = int(row[13])
            row[14] = int(row[14])
            row[16] = int(row[16])
            row[17] = int(row[17])
            row[22] = int(row[22])
            row[23] = int(row[23])
            row[26] = int(row[26])
            row[27] = int(row[27])
            row[31] = int(row[31])
            row[32] = int(row[32])
            row[33] = int(row[33])

            row[24] = pd.Timestamp(row[24]).to_pydatetime()

        # Imprime la fila que se va a insertar
            print(f"Insertando fila: {tuple(row)}")
            cursor.execute(sql, tuple(row))
            
        connection.commit()
    except Exception as e:
        titulo = "Error en query insertar requisicion"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    finally:
        cursor.close()
        connection.close()

    return max_cve_doc

def max_partida_requi(max_cve_doc):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
    # Example query
    sql = f'''SELECT COALESCE(MAX(NUM_PAR), 0) FROM {database_05}.dbo.PAR_COMPQ05 WHERE CVE_DOC = ?'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, max_cve_doc)

        max_partida = cursor.fetchone()[0]
        max_partida = int(max_partida)

        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

    return max_partida

def unidad_partida(items_df):
    cve_art = items_df["skuvari"].iloc[0]

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
    sql = f'''SELECT [UNI_MED] FROM {database_05}.dbo.INVE05 where CVE_ART = ? '''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, cve_art)

        unidad = cursor.fetchone()[0]

        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    
    return unidad

def insert_part_req(items_df, max_cve_doc, unidad):
    num_par = max_partida_requi(max_cve_doc)
    print(num_par)
    num_par = num_par+1

    max_cve_doc = " " * 10 + str(max_cve_doc).zfill(10)
    print(max_cve_doc)

    cve_art = items_df["skuvari"].iloc[0]
    cant = items_df["cantidad"].iloc[0]

    part_requi = pd.DataFrame(columns = ["CVE_DOC", "NUM_PAR", "CVE_ART", "CANT", "PXR", "PREC", "COST", "IMPU1", "IMPU2", "IMPU3", "IMPU4", 
                                         "IMP1APLA", "IMP2APLA", "IMP3APLA", "IMP4APLA", "TOTIMP1", "TOTIMP2", "TOTIMP3", "TOTIMP4", "DESCU", 
                                         "ACT_INV", "TIP_CAM", "UNI_VENTA", "TIPO_ELEM", "TIPO_PROD", "CVE_OBS", "REG_SERIE", "E_LTPD", 
                                         "FACTCONV", "NUM_ALM", "MINDIRECTO", "NUM_MOV", "TOT_PARTIDA", "MAN_IEPS", "APL_MAN_IMP", "CUOTA_IEPS", 
                                         "APL_MAN_IEPS", "MTO_PORC", "MTO_CUOTA", "CVE_ESQ"])
    
    data = {
        "CVE_DOC": max_cve_doc,
        "NUM_PAR": num_par,
        "CVE_ART": cve_art,
        "CANT": cant,
        "PXR": 0,
        "PREC": 0,
        "COST": 0,
        "IMPU1":0,
        "IMPU2":0,
        "IMPU3":0,
        "IMPU4":0,
        "IMP1APLA": 6,
        "IMP2APLA": 6,
        "IMP3APLA": 6,
        "IMP4APLA": 0,
        "TOTIMP1": 0,
        "TOTIMP2": 0,
        "TOTIMP3": 0,
        "TOTIMP4": 0,
        "DESCU": 0,
        "ACT_INV": "N",
        "TIP_CAM": 1,
        "UNI_VENTA": unidad, #REVISARLO,
        "TIPO_ELEM": "N",
        "TIPO_PROD": "P",
        "CVE_OBS": 0,
        "REG_SERIE": 0,
        "E_LTPD": 0, 
        "FACTCONV": 1,
        "NUM_ALM": 1,
        "MINDIRECTO": 0,
        "NUM_MOV": 0,
        "TOT_PARTIDA": 0,
        "MAN_IEPS": "N",
        "APL_MAN_IMP": 1,
        "CUOTA_IEPS": 0,
        "APL_MAN_IEPS": "C",
        "MTO_PORC": 0,
        "MTO_CUOTA": 0,
        "CVE_ESQ": 1}
    
    part_requi.loc[len(part_requi)] = data

    lista =  [tuple(x) for x in part_requi.to_records(index=False)]
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
            # Example query
    sql = f'''INSERT INTO {database_05}.dbo.PAR_COMPQ05 
             ([CVE_DOC], [NUM_PAR], [CVE_ART], [CANT], [PXR], [PREC], [COST], [IMPU1], [IMPU2], [IMPU3], [IMPU4],
             [IMP1APLA], [IMP2APLA], [IMP3APLA], [IMP4APLA], [TOTIMP1], [TOTIMP2], [TOTIMP3], [TOTIMP4], [DESCU], 
             [ACT_INV], [TIP_CAM], [UNI_VENTA], [TIPO_ELEM], [TIPO_PROD], [CVE_OBS], [REG_SERIE], [E_LTPD], 
             [FACTCONV], [NUM_ALM], [MINDIRECTO], [NUM_MOV], [TOT_PARTIDA], [MAN_IEPS], [APL_MAN_IMP], [CUOTA_IEPS], 
             [APL_MAN_IEPS], [MTO_PORC], [MTO_CUOTA], [CVE_ESQ])
             VALUES (?,?,?,?,?,?,?,?,?,?,
                     ?,?,?,?,?,?,?,?,?,?,
                     ?,?,?,?,?,?,?,?,?,?,
                     ?,?,?,?,?,?,?,?,?,?)'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        for row in lista:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            #give correct type to the rows
            # row[0] = int(row[0])
            row[1] = int(row[1])
            row[3] = int(row[3])
            row[4] = int(row[4])
            row[5] = int(row[5])
            row[6] = int(row[6])
            row[7] = int(row[7])
            row[8] = int(row[8])
            row[9] = int(row[9])
            row[10] = int(row[10])
            row[11] = int(row[11])
            row[12] = int(row[12])
            row[13] = int(row[13])
            row[14] = int(row[14])
            row[15] = int(row[15])
            row[16] = int(row[16])
            row[17] = int(row[17])
            row[18] = int(row[18])
            row[19] = int(row[19])
            row[21] = int(row[21])
            row[25] = int(row[25])
            row[26] = int(row[26])
            row[27] = int(row[27])
            row[28] = int(row[28])
            row[29] = int(row[29])
            row[30] = int(row[30])
            row[31] = int(row[31])
            row[32] = int(row[32])
            row[34] = int(row[34])
            row[35] = int(row[35])
            row[37] = int(row[37])
            row[38] = int(row[38])
            row[39] = int(row[39])

           
        # Imprime la fila que se va a insertar
            print(f"Insertando fila: {tuple(row)}")
            cursor.execute(sql, tuple(row))
            
        connection.commit()
    except Exception as e:
        titulo = "Error en query insertar requisicion"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)
    finally:
        cursor.close()
        connection.close()

def requi_lista(folio, skuvari, estado_nuevo):

     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]

    sql = f'''UPDATE [SlicTe].[dbo].[STe_Falta]
              SET N_S = ?
              Where Folio = ? AND skuvari = ? '''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (estado_nuevo, folio, skuvari))
        connection.commit()
        
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)

def ctrl_requi():
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
    sql_select = f'''SELECT ULT_CVE from {database_05}.dbo.TBLCONTROL05 WHERE ID_TABLA = 32'''
    sql_update = f'''UPDATE {database_05}.dbo.TBLCONTROL05 
                     SET ULT_CVE = ?
                     WHERE ID_TABLA = 32'''

    try:
        # Use a context manager to handle the connection and cursor
        first_connector = connectors[0]
        with first_connector.connect() as connection:
            with connection.cursor() as cursor:
                # Execute the SELECT query
                cursor.execute(sql_select)
                value = cursor.fetchone()[0]
                
                # Increment the value
                value += 1
                
                # Execute the UPDATE query
                cursor.execute(sql_update, value)
                
                # Commit the transaction to save changes
                connection.commit()
                
                # Print the new value
                print(value)

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

def inve_program():

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    lista_productos = []
    sql  = f'''SELECT DISTINCT CVE_ART FROM {database_04}.[dbo].[INVE04]'''

    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql)
        lista_productos = cursor.fetchall()
        lista_productos = [row[0] for row in lista_productos]
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

    return lista_productos

def foliosc05():

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database=f"{database_05}"
        )]
    
    sql_select = f'''SELECT ULT_DOC from {database_05}.dbo.FOLIOSC05 WHERE TIP_DOC = 'q' '''
    sql_update = f'''UPDATE {database_05}.dbo.FOLIOSC05 
                     SET ULT_DOC = ?
                     WHERE TIP_DOC = 'q' '''

    try:
        # Use a context manager to handle the connection and cursor
        first_connector = connectors[0]
        with first_connector.connect() as connection:
            with connection.cursor() as cursor:
                # Execute the SELECT query
                cursor.execute(sql_select)
                value = cursor.fetchone()[0]
                
                # Increment the value
                value += 1
                
                # Execute the UPDATE query
                cursor.execute(sql_update, value)
                
                # Commit the transaction to save changes
                connection.commit()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)

def PT_falta(folio):


     # Define multiple instances for different databases
    connectors = [
        SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=f"{servidor}",
            database="SlicTe"
        )
    ]

    sql = f'''SELECT 
                    COALESCE(p.[skuvari], o.[skuvari]) AS skuvari,
                    COALESCE(p.[desvari], o.[desvari]) AS desvari,
                    p.cant AS prod_cant
                FROM (
                    SELECT 
                        [skuvari],
                        [desvari],
                        SUM([cantnec]) AS cant
                    FROM [SlicTe].[dbo].[STe_aProd]
                    WHERE Folio = ?
                    GROUP BY [skuvari], [desvari]
                ) AS p
                FULL OUTER JOIN (
                    SELECT 
                        [skuvari],
                        [desvari],
                        SUM([cant]) AS cant
                    FROM [SlicTe].[dbo].[STe_OP]
                    WHERE Folio = ?
                    GROUP BY [skuvari], [desvari]
                ) AS o
                ON p.[skuvari] = o.[skuvari] AND p.[desvari] = o.[desvari]
                WHERE p.cant <> o.cant 
                OR p.cant IS NULL 
                OR o.cant IS NULL
                ORDER BY skuvari DESC'''
    
    sql_por_entrega = f'''INSERT INTO SlicTe.dbo.STe_PorEntrega 
                            ([skuvari], [desvari] ,[cantidad] ,[folio] ,[N_S])
                            VALUES(?,?,?,?,?)'''
    lista_PT_falta = []
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (folio, folio))
        
        lista_PT_falta = cursor.fetchall()
        lista_PT_falta = [list(row) for row in lista_PT_falta]

        df_pt_falta = pd.DataFrame(lista_PT_falta, columns = ["skuvari", "desvari", "cantidad"])
        df_pt_falta["folio"] = folio
        df_pt_falta["N_S"] = "Falta_Materia_Prima"

        cursor.close()
        connection.close()

        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        lista =  [tuple(x) for x in df_pt_falta.to_records(index=False)]

        for row in lista:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[3] = int(row[3])            
            cursor.execute(sql_por_entrega, row)
            
        connection.commit()
        
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

    finally:
        # Ensure the connection is closed even if an error occurs
        if cursor:
            cursor.close()
        if connection:
            connection.close()

def insert_compras(items_df):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]
    sql = f'''INSERT INTO SlicTe.dbo.STe_OP
                    ([Folio],[skuvari],[desvari],[cant]
                    ,[N_S],[OP],[lote], [consumo],[tipo],[receta])
                    VALUES (?,?,?,?,
                            ?,?,?,?,
                            ?, ?)'''
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        lista_pp =  [tuple(x) for x in items_df.to_records(index=False)]

        for row in lista_pp:
            # Ensure the row is a list and convert the fields accordingly
            row = list(row)

            row[0] = int(row[0])
            row[3] = int(row[3])
            row[5] = int(row[5])  

            cursor.execute(sql, tuple(row))
            connection.commit()
        
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)
    
def update_estado_compras(n_s, skuvari, folio):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=f"{servidor}",
        database="SlicTe"
        )]

    sql = f'''UPDATE [SlicTe].[dbo].[STe_PorEntrega]
              SET N_S = ?
              Where skuvari = ? AND folio = ?'''
    n_s = str(n_s)  # Ensure n_s is an integer
    skuvari = str(skuvari)  # Ensure skuvari is a string
    folio = int(folio)  # Ensure folio is a string
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql, (n_s, skuvari, folio))
        connection.commit()
        
        cursor.close()
        connection.close()
    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        print(e)
        messagebox.showerror(titulo, mensaje)


    


    

    

    















    

    

