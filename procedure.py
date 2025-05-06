from conexion_db import SQLServerConnector
import pyodbc as podbc
import pandas as pd
from tkinter import messagebox
from tabulate import tabulate
from datetime import datetime
import numpy as np

def query_exitmp(folio):
    # Create a SQLServerConnector instance
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=r"MARIO\SQLMARIO",
        database="SlicTe"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=r"MARIO\SQLMARIO",
        database="Empre04SQL"
        ),
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=r"MARIO\SQLMARIO",
        database="Empre05SQL"
        )]
    
    STe_ExiTMP = []
    STe_pedidos = []
    materia_prima = []
    STe_OP = []
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
                    Empre04SQL.dbo.MULT04 I 
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
           From Empre05SQL.dbo.KITS05 P, Empre05SQL.dbo.INVE05 I
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
    dfexi_temp = pd.DataFrame(STe_ExiTMP, columns = ["folio", "skuvari", "desvari", "cant", "exist", "stmin"])
    df_pedidos = pd.DataFrame(STe_pedidos, columns = ["folio", "origen", "nopedido", "skuvari", "desvari", "cant"])
    df_materia_prima = pd.DataFrame(materia_prima, columns = ["CVE_ART", "CVE_PROD", "DESCR", "CANTIDAD", "EXIST", "STOCK_MIN"])
    df_OP = pd.DataFrame(columns = ["OP", "cant", "skuvari", "desvari", "lote", "consumo", "folio", "nopedido", "origen"])
   

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

def compra_falta(folio):

    df_aprod, df_asurtir, df_materia_prima, df_OP = query_exitmp(folio)

    df_aprod_agg = df_aprod.groupby(['folio', 'origen', 'skuvari', 'desvari']).agg({'cant_nec': 'sum'}).reset_index()
    
    max_op = df_OP["OP"].max()
    v_nextOP = max_op +1 if not np.isnan(max_op) else 0 +1
    v_lote = num_lote()
    v_caducidad = caducidad()

    a_falta = pd.DataFrame(columns = ["folio", "sku_vari", "desvari", "cant", "exist", "ban"])
    
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
                
                # Append the row to a_falta DataFrame
                    a_falta = a_falta._append({
                        'folio': folio_prod_value,
                        'sku_vari': cve_prod_value, # or 'CVE_ART' if that's what you intend
                        'desvari': descr_value,
                        'cant': cant_materia_value,
                        'exist': exist_materia_value,
                        'ban': v_ban
                    }, ignore_index=True)
                
                if exist_materia_value < cant_materia_value and v_ban != "M":

                    v_ban = "F"
                    a_falta = a_falta._append({
                        'folio': folio_prod_value,
                        'sku_vari': cve_prod_value, #no es la cve ART?
                        'desvari': descr_value,
                        'cant': cant_materia_value,
                        'exist': exist_materia_value,
                        'ban': v_ban
                        }, ignore_index = True)
                    
        if v_ban == "S":
                df_OP = df_OP._append({
                    'OP': v_nextOP,
                    'cant': cant_prod_value,
                    'skuvari':skuvari_prod_value,
                    'desvari': desvari_prod_value,
                    'lote':v_lote,
                    'consumo': v_caducidad,
                    'folio': folio_prod_value,
                    'origen':origen_prod_value
                }, ignore_index = True)
    return df_aprod, df_asurtir,a_falta, df_OP

def insert_procedure(folio):
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
    

      # Convert to native Python types
    df_falta = df_falta.astype({
        'folio': 'int64',
        'sku_vari': 'object',
        'desvari': 'object',
        'cant': 'int64',
        'exist': 'int64',
        'ban': 'object'
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
        'origen': 'object'
    })

    lista_OP =  [tuple(x) for x in df_OP.to_records(index=False)]

    

    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=r"MARIO\SQLMARIO",
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
                    ([folio] ,[skuvari] ,[desvari] ,[cant] ,[exist] ,[ban])
              VALUES(?, ?, ?, ?, ?, ?)'''
    
    sql_OP = f'''INSERT INTO  [SlicTe].[dbo].[STe_OP] 
                    ([OP] ,[cant] ,[skuvari] ,[desvari] ,[lote] ,[consumo], [folio], [nopedido], [origen])
              VALUES(?, ?, ?, ?, ?, ?, ?, ?, ?)'''
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
        titulo = "Error en query"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
        print(e)

def query_busqueda_op(folio):

    v_aprod, v_asurtir, v_falta, v_op = query_condicion(folio)

    if all(var == 0 for var in [v_aprod, v_asurtir, v_falta, v_op]):

        insert_procedure(folio)
        connectors = [
            SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=r"MARIO\SQLMARIO",
            database="SlicTe"
            )]
    
        lista_op = []
        # Example query
        sql = f'''SELECT OP, cant, skuvari, desvari, lote, consumo
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
        return lista_op
    else:
        connectors = [
            SQLServerConnector(
            driver="{SQL Server Native Client 11.0}",
            server=r"MARIO\SQLMARIO",
            database="SlicTe"
            )]
    
        lista_op = []
        # Example query
        sql = f'''SELECT OP, cant, skuvari, desvari, lote, consumo
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
        return lista_op

def query_condicion(folio):
    connectors = [
        SQLServerConnector(
        driver="{SQL Server Native Client 11.0}",
        server=r"MARIO\SQLMARIO",
        database="SlicTe"
        )]
    

    # Example query
    sql_aprod = f'''SELECT Count (*)
                    FROM SlicTe.dbo.STe_aProd 
                    WHERE Folio = ? '''
    sql_asurtir = f'''SELECT Count (*)
                    FROM SlicTe.dbo.STe_aSurtir
                    WHERE Folio = ? '''
    sql_falta = f'''SELECT Count (*)
                    FROM SlicTe.dbo.STe_Falta
                    WHERE Folio = ? '''
    sql_op = f'''SELECT Count (*)
                    FROM SlicTe.dbo.STe_OP
                    WHERE Folio = ? '''
    

        # Run the example query for each database
    try:
        first_connector = connectors[0]
        connection = first_connector.connect()
        cursor = connection.cursor()
        cursor.execute(sql_aprod, folio)

        v_aprod = cursor.fetchone()[0]

        cursor.execute(sql_asurtir, folio)
        v_asurtir = cursor.fetchone()[0]

        cursor.execute(sql_falta, folio)
        v_falta = cursor.fetchone()[0]

        cursor.execute(sql_op, folio)
        v_op = cursor.fetchone()[0]

        # Close the cursor and connection
        cursor.close()
        connection.close()

    except Exception as e:
        titulo = "Error"
        mensaje = f"Error: {str(e)}"
        messagebox.showerror(titulo, mensaje)
    return v_aprod, v_asurtir, v_falta, v_op
    
    
        


folio = 5
x1,x2,x3,x4 = query_condicion(folio)
print(x1,x2,x3,x4)

# query_busqueda_op(folio)



# Example usage
# folio = 2
# df_1, df_2, df_3, df_4 = query_exitmp(folio)

# df_aprod_agg = df_1.groupby(['folio', 'origen', 'skuvari', 'desvari']).agg({'cant_nec': 'sum'}).reset_index()

# sku_value = df_aprod_agg.iloc[1]['skuvari']
# print(sku_value)
# sku_value = sku_value.strip()

# filtered_df = df_3[df_3['CVE_ART'] == str(sku_value)]
# print(filtered_df)

# df_1, df_2, df_3, df_4, v = compra_falta(2)
# print(df_4, v)
# lista_aprod =  [tuple(x) for x in df_1.to_records(index=False)]
# print(lista_aprod)

# print(df_1.dtypes)

# x = num_lote()
# print(x)

# Print column names and first few rows
# print("STe_ExiTMP column names:")
# print(len(df_1))
# print(len(df_2))
# print(len(df_3))
# print(len(df_4))
# print(len(df_materia_prima))
# filtered_df = df_materia_prima[(df_materia_prima['CVE_ART'] == 'TS51030')]
# filtered_df['CANTIDAD'] = filtered_df['CANTIDAD']*2
# # # summary_df = filtered_df['CANTIDAD'].sum()
# print(filtered_df)
# # print("longitud producir:")
# print(df_aprod)

# Group by folio, origen, skuvari, desvari and calculate the sum of cantnec
# df_aprod_grouped = df_aprod.groupby(['folio', 'origen', 'skuvari', 'desvari']).agg({'cant_nec': 'sum'}).reset_index()
# print(df_aprod_grouped)

# print(df_aprod)

# print("\nSTe_Pedidos column names:")
# print(df_pedidos.columns)
# print("First few rows of STe_Pedidos:")
# print(df_pedidos.head())