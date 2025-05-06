import tkinter as tk
from tkinter import ttk
import customtkinter as ctk
from PIL import Image, ImageTk
from datetime import datetime, timedelta
import pandas as pd
from tkinter import ttk
from tkinter import messagebox
from model.querys_dao_2 import afacta, fact_clib, general_factc, impuestos_producto, insert_bita, insert_into_pedweb, lista_prod, lista_pedidos, max_cvedoc_factc, modif_exist_inve04, modif_exist_mult04, par_factc, parfactc_clib, query_busqueda_op, query_asurtir, query_op, query_acomprar, query_archivo_pedidos, query_ctrl_orden_produccion, salidas, insert_salidas, entradas, insert_entradas, select_cvebita, select_detalle, select_existg, select_ped_web, update_ctrl_05, insert_exist_entradas, list_of_names, guardar, update_folios_fac, update_pedweb, update_tab_ctrl, variables_inventarios_entradas, requi_lista, ctrl_requi, foliosc05, web_movinve
from model.querys_dao_2 import query_descargar_pedidos, query_list_all_pedidos, query_insert_new, query_login, query_nuevo_usuario, query_desc_web, select_usuario, producido, materia_prima, variables_salidad_inve, variables_entradas_inve, consumo_nuevo, insert_exist_salidas, update_ctrl_04, almacen_04, requi_compra, insert_part_req,unidad_partida, update_estado_compras
from model.querys_dao_2 import agregar_productos_pp, listar_pp, seleccionar_pp,  eliminar_pp, pedi_05, calc_max_ltpd04,convertir_fecha, buscar_pedi_04, select_lista_pedimentos, insert_ltpd04, update_ltpd05, modificar_lote_nuevo, query_clave_admin, listar_users, eliminar_user, query_ctrl_pt_falta, des_product, inve_program, num_max_usuario, max_op, recetas, insert_compras
import pyodbc as podbc
from tkinter import filedialog
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

def barra_menu(root):
    barra_menu = tk.Menu(root)
    root.config(menu = barra_menu,
                width = 300,
                height = 300)
    #first object of the menu INICIO
    menu_inicio = tk.Menu(barra_menu, tearoff = 0)
    barra_menu.add_cascade(label = "Inicio", menu = menu_inicio)
   
    
    #add commands to inicio
    menu_inicio.add_command(label = "Configurar base de datos", command = lambda: open_conection_table(root))

class sql_connection_parameters(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.geometry("477x300")
        self.resizable(0,0)
        self.columnconfigure((0, 1, 2), weight=1)
        self.rowconfigure((0, 1, 2, 3), weight=1)

        self.title("Conexión a base de datos")

        text_label = tk.Label(self, text="Conexión a Base de Datos")
        text_label.config(font=("Roboto", 20, "bold"), anchor="center")
        text_label.grid(row=0, column=1, padx=10, pady=10)

        self.campos_frame = tk.Frame(self)
        self.campos_frame.grid(row = 1, column = 0, columnspan = 3)

        self.campos_registro()
        

    def campos_registro(self):
        # servidor label
        servidor_label = tk.Label(self.campos_frame, text="Servidor SQL")
        servidor_label.config(font=("Roboto", 16, "bold"), anchor="center")
        servidor_label.grid(row=0, column=0)
    # # servidor entry
        self.my_servidor = tk.StringVar()
        servidor_entry = tk.Entry(self.campos_frame, textvariable=self.my_servidor)
        servidor_entry.config(width=12, font=("Roboto", 16))
        servidor_entry.grid(row=0, column=1, sticky="nsew")

        #base de datos slic
        base04_label = tk.Label(self.campos_frame, text="Base de datos 04")
        base04_label.config(font=("Roboto", 16, "bold"), anchor="center")
        base04_label.grid(row=1, column=0, padx=10, pady=10)

         #base de datos slic entry
        self.my_base04 = tk.StringVar()
        base04_entry = tk.Entry(self.campos_frame, textvariable=self.my_base04)
        base04_entry.config(width=12, font=("Roboto", 16))
        base04_entry.grid(row=1, column=1, sticky="se", pady = 5, padx = 5)

        #base de datos slic
        base05_label = tk.Label(self.campos_frame, text="Base de datos 05")
        base05_label.config(font=("Roboto", 16, "bold"), anchor="center")
        base05_label.grid(row=2, column=0, padx=10, pady=10)

         #base de datos slic entry
        self.my_base05 = tk.StringVar()
        base05_entry = tk.Entry(self.campos_frame, textvariable=self.my_base05)
        base05_entry.config(width=12, font=("Roboto", 16))
        base05_entry.grid(row=2, column=1, sticky="se", pady = 5, padx = 5)

        # Guardar datos
        guardar_button = ctk.CTkButton(self.campos_frame, text="Guardar Datos", command = self.registrar_datos_conexion)
        guardar_button.configure(width=100, height=50, font=("Roboto", 16, "bold"), anchor="center", border_color = "black",
                               border_width = 2)
        guardar_button.grid(row=3, column=0, padx=10, pady=10, columnspan = 2)

    def registrar_datos_conexion (self):
        lista_conexion_db = [[self.my_servidor.get(),
                            self.my_base04.get(),
                            self.my_base05.get()]]
        conexion_df = pd.DataFrame(lista_conexion_db, columns = ["servidor", "base04", "base05"])

        
        try:
            file_path = resource_path(f"client/conexion.csv")
            conexion_df.to_csv(file_path)
            title = "Párametros de conexión"
            message = "Se Guardaron los parámetros con éxito"
            messagebox.showinfo(title, message)
            self.destroy()

        except Exception as e:
            titulo = "Error en funcion"
            mensaje = f"Error: {str(e)}"
            messagebox.showerror(titulo, mensaje)
            print(e)

def open_conection_table(master):
    ventana_conection_open = sql_connection_parameters(master)

def main():
    # Create the root window
    root = tk.Tk()
    # Change the title of the window
    root.title("SLIC-IATI_PROD 2.0")
    # Add the logo
    icon_path = resource_path("img/Picture1.ico")
    root.iconbitmap(icon_path)
    # Change the size of the window
    # root.resizable(0, 0)
    root.geometry("1309x766")
    barra_menu(root)

    # Create the login window
    login_window = LoginWindow(root)

    # Start the Tkinter main loop
    root.mainloop()

class LoginWindow(tk.Frame):
    def __init__(self, master):
        super().__init__(master)
        self.pack(fill="both", expand=True)
        self.columnconfigure((0, 1, 2), weight=1)
        self.rowconfigure((0, 1, 2, 3), weight=1)

        # Logo
        logo_path = resource_path("img/logo_login_screen.jpg")
        logo = Image.open(logo_path)
        resized_image = logo.resize((200, 200), Image.Resampling.LANCZOS)
        self.converted_image = ImageTk.PhotoImage(resized_image)  # Keep a reference to avoid garbage collection
        label_image = tk.Label(self, image=self.converted_image, width=200, height=200, bg="black", fg="yellow")
        label_image.grid(row=0, column=1)

        # Login frame
        login_frame = ttk.Frame(self)
        login_frame.grid(column=1, row=1)

        # Label
        login_label = tk.Label(login_frame, text="Iniciar sesión")
        login_label.config(font=("Roboto", 30, "bold"), anchor="center")
        login_label.grid(row=0, column=0, padx=10, pady=10, sticky="nsew", columnspan=2)

        # User label
        user_label = tk.Label(login_frame, text="Usuario")
        user_label.config(font=("Roboto", 16, "bold"), anchor="center")
        user_label.grid(row=1, column=0, padx=10, pady=10)

        # User entry
        self.my_user = tk.StringVar()
        user_entry = tk.Entry(login_frame, textvariable=self.my_user)
        user_entry.config(width=12, font=("Roboto", 16))
        user_entry.grid(row=1, column=1, padx=5, pady=5, sticky="nsew")

        # Password label
        password_label = tk.Label(login_frame, text="Contraseña")
        password_label.config(font=("Roboto", 16, "bold"), anchor="center")
        password_label.grid(row=2, column=0, padx=10, pady=10)

        # Password entry
        self.my_password = tk.StringVar()
        password_entry = tk.Entry(login_frame, textvariable=self.my_password, show="*")
        password_entry.config(width=12, font=("Roboto", 16))
        password_entry.grid(row=2, column=1, padx=5, pady=5, sticky="nsew")

        # Login button
        login_button = ctk.CTkButton(login_frame, text="Iniciar sesión", command=self.open_new_window)
        login_button.configure(width=100, height=50, font=("Roboto", 16, "bold"), anchor="center", border_color = "black",
                               border_width = 2)
        login_button.grid(row=3, column=0, padx=10, pady=10)

        #new user
        new_user_button = ctk.CTkButton(login_frame, text="Registrar Nuevo\nUsuario", command =  self.open_admin_password)
        new_user_button.configure(width=100, height=50, font=("Roboto", 16, "bold"), anchor="center",
                                  fg_color = "#418236", hover_color = "#205816", border_color = "black", border_width = 2)
        new_user_button.grid(row=3, column=1, padx=10, pady=10)

    def open_new_window(self):
        try:
           # Validate the login credentials
            if query_login(self.my_user.get(), self.my_password.get()):
                # If login is successful, switch to opcioneswindow
                hide_all_frames(self.master)
                niv_user = select_usuario(self.my_user.get())
                opciones_window = opcioneswindow(self.master, niv_user)
                opciones_window.pack(fill="both", expand=True)
            else:
                titulo = "Inicio de sesión"
                mensaje = "Usuario o Contraseña incorrectos"
                messagebox.showerror(titulo, mensaje)

        except Exception as e:
            titulo = "Error en funcion"
            mensaje = f"No se pudo iniciar sesión: {str(e)}"
            messagebox.showerror(titulo, mensaje)
            print(e)
    def open_admin_password(self):
        admin_valid = window_admin_valid(self.master)
    def open_registro(self):
         registro_open = Registro_usuario(self.master)

class window_admin_valid(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.geometry("477x200")
        self.resizable(0,0)
        self.columnconfigure((0, 1, 2), weight=1)
        self.rowconfigure((0, 1, 2, 3), weight=1)

        self.title("Clave Admin.")

        self.campos_frame = tk.Frame(self)
        self.campos_frame.grid(row = 0, column = 0, columnspan = 3)

        self.campos_registro()
        self.grab_set()
    def campos_registro(self):

        # User label
        user_label = tk.Label(self.campos_frame, text="Usuario:")
        user_label.config(font=("Roboto", 16, "bold"), anchor="center")
        user_label.grid(row=0, column=0, padx = 5, pady = 5)
    # # User entry
        self.my_useradmin = tk.StringVar()
        admin_entry = tk.Entry(self.campos_frame, textvariable=self.my_useradmin)
        admin_entry.config(width=12, font=("Roboto", 16))
        admin_entry.grid(row=0, column=1, sticky="nsew", padx = 5, pady = 5)

        # User label
        clave_label = tk.Label(self.campos_frame, text="Clave Administrador:")
        clave_label.config(font=("Roboto", 16, "bold"), anchor="center")
        clave_label.grid(row=1, column=0)

    # # User entry
        self.my_passadmin = tk.StringVar()
        user_entry = tk.Entry(self.campos_frame, textvariable=self.my_passadmin, show = "*")
        user_entry.config(width=12, font=("Roboto", 16))
        user_entry.grid(row=1, column=1, sticky="nsew")

        # Guardar usuario button
        confirmar_clave_button = ctk.CTkButton(self.campos_frame, text="Confirmar\nClave", command=self.confirm_clave)
        confirmar_clave_button.configure(width=100, height=50, font=("Roboto", 16, "bold"), anchor="center", border_color = "black",
                               border_width = 2)
        confirmar_clave_button.grid(row=2, column=0, padx=10, pady=10, columnspan = 2)
    def confirm_clave (self):
        try:
# Validate the login credentials
            self.admin = self.my_useradmin.get()
            self.admin_pass = self.my_passadmin.get()
            claves = query_clave_admin()
            claves = claves[0]
            claves = [name.strip() for name in claves]
            x = claves[0]
            y = claves[1]
            if x == self.admin and y == self.admin_pass:
                # If login is successful, switch to opcioneswindow
                self.destroy()
                registro_open = Registro_usuario(self.master)
            else:
                titulo = "Clave de Administrador"
                mensaje = "Usuario o Contraseña incorrectos"
                messagebox.showerror(titulo, mensaje)

        except Exception as e:
            titulo = "Error en funcion"
            mensaje = f"Error: {str(e)}"
            messagebox.showerror(titulo, mensaje)
            print(e)
    def open_registro(self):
        registro_open = Registro_usuario(self.master)

class Registro_usuario(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        self.geometry("477x300")
        self.resizable(0,0)
        self.columnconfigure((0, 1, 2), weight=1)
        self.rowconfigure((0, 1, 2, 3), weight=1)
        self.grab_set()

        self.title("Nuevo Usuario")
    # Add the logo
        # self.iconbitmap("img/eurote_logo.ico")

        nuevo_usuario_label = tk.Label(self, text="Registro de nuevo usuario")
        nuevo_usuario_label.config(font=("Roboto", 20, "bold"), anchor="center")
        nuevo_usuario_label.grid(row=0, column=1, padx=10, pady=10)

        self.campos_frame = tk.Frame(self)
        self.campos_frame.grid(row = 1, column = 0, columnspan = 3)

        self.campos_registro()

        

    def campos_registro(self):
    #     # User label
    #     user_label = tk.Label(self.campos_frame, text="Usuario (Número)")
    #     user_label.config(font=("Roboto", 16, "bold"), anchor="center")
    #     user_label.grid(row=0, column=0)
    # # # User entry
    #     self.my_nuevo_usuario = tk.StringVar()
    #     user_entry = tk.Entry(self.campos_frame, textvariable=self.my_nuevo_usuario)
    #     user_entry.config(width=12, font=("Roboto", 16))
    #     user_entry.grid(row=0, column=1, sticky="nsew")

        #Nombre de usuario
        nombre_label = tk.Label(self.campos_frame, text="Nombre")
        nombre_label.config(font=("Roboto", 16, "bold"), anchor="center")
        nombre_label.grid(row=1, column=0, padx=10, pady=10)

         # nombre de usuario entry
        self.my_nombre = tk.StringVar()
        nombre_usuario_entry = tk.Entry(self.campos_frame, textvariable=self.my_nombre)
        nombre_usuario_entry.config(width=12, font=("Roboto", 16))
        nombre_usuario_entry.grid(row=1, column=1, sticky="se", pady = 5, padx = 5)

        #Nivel de usuario
        nivel_label = tk.Label(self.campos_frame, text="Nivel de usuario")
        nivel_label.config(font=("Roboto", 16, "bold"), anchor="center")
        nivel_label.grid(row=2, column=0, padx=10, pady=10)

         # nombre de usuario entry
        self.my_nivel = tk.StringVar()
        nivel_usuario_entry = tk.Entry(self.campos_frame, textvariable=self.my_nivel)
        nivel_usuario_entry.config(width=12, font=("Roboto", 16))
        nivel_usuario_entry.grid(row=2, column=1, sticky="se", pady = 5, padx = 5)

        # Password label
        password_label = tk.Label(self.campos_frame, text="Contraseña")
        password_label.config(font=("Roboto", 16, "bold"), anchor="center")
        password_label.grid(row=3, column=0, padx=10, pady=10)

         # Password entry
        self.my_password = tk.StringVar()
        password_entry = tk.Entry(self.campos_frame, textvariable=self.my_password, show="*")
        password_entry.config(width=12, font=("Roboto", 16))
        password_entry.grid(row=3, column=1, sticky="se", pady = 5, padx = 5)

        # Guardar usuario button
        guardar_usuario_button = ctk.CTkButton(self.campos_frame, text="Guardar Nuevo\nUsuario", command = self.registrar_nuevo_usuario)
        guardar_usuario_button.configure(width=100, height=50, font=("Roboto", 16, "bold"), anchor="center", border_color = "black",
                               border_width = 2)
        guardar_usuario_button.grid(row=4, column=0, padx=10, pady=10, columnspan = 2)

    def registrar_nuevo_usuario (self):
        name = self.my_nombre.get()
        lista_user_names = list_of_names()
        v_max = num_max_usuario()
        v_max = v_max+1
        if name in lista_user_names:
            messagebox.showerror("ERROR!", "Nombre de usuario existente, usar otro nombre")
        else:
            lista_nuevo_usuario = [[
                                self.my_nombre.get(),
                                self.my_nivel.get(),
                                self.my_password.get(),
                                v_max]]
            try:
                query_nuevo_usuario(lista_nuevo_usuario)
                title = "Registro de nuevo usuario"
                message = "Se registro el nuevo usuario con éxito"
                messagebox.showinfo(title, message)
                self.destroy()

            except Exception as e:
                titulo = "Error en funcion"
                mensaje = f"Error: {str(e)}"
                messagebox.showerror(titulo, mensaje)
                print(e)

class opcioneswindow(tk.Frame):
    def __init__(self, master, niv_user):
        super().__init__(master)

        self.user_niv = niv_user

        self.columnconfigure((0,1,2), weight = 1)
        self.rowconfigure((0,1,2,3), weight = 1)

        login_label= tk.Label(self, text = "Comercializadora Sueca de México, S.A. DE C.V.")
        login_label.config(font=("Roboto", 22, "bold"), anchor="center")
        login_label.grid(row = 0, column = 0, columnspan=3)

        #dia de trabajo
        self.date_label = tk.Label(self, text = "")
        self.date_label.grid(row = 1, column= 0, columnspan=3)
        self.date_label.config(font=("Roboto", 22, "bold"), anchor="center")

        #frame for all the options
        self.opciones_frame = tk.Frame(self)
        self.opciones_frame.grid(row = 2, column = 1, padx=10, pady =10)

        self.salir_frame = tk.Frame(self)
        self.salir_frame.grid(row = 3, column = 1, padx=10, pady =10)

        self.opciones()
        self.update_date()

    def update_date(self):
     # Get the current date and format it
        today_date = datetime.now().strftime("%Y-%m-%d")

    # Update the label text
        self.date_label.config(text="Día de trabajo: " + today_date)

    def opciones(self):
        #options
        #img consolidar pedidos
        img_consolidar_pedidos_path = resource_path("img/consolidar_pedidos.png")
        img_consolidar_pedidos = Image.open(img_consolidar_pedidos_path)
        resized_img_consolidar = img_consolidar_pedidos.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_image = ImageTk.PhotoImage(resized_img_consolidar)

        #buton consolidar pedidos
        self.button_consolidar_pedidos = ctk.CTkButton(self.opciones_frame, image = self.converted_image, text = "Consolidar Pedidos", 
                                                  command = self.open_consolidar_pedidos_window, state="disabled")
        self.button_consolidar_pedidos.configure(width = 300, height = 50,
                                            fg_color ="#3B8ED0", text_color="black", hover_color="#36719F",
                                            border_width = 2,
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.button_consolidar_pedidos.grid(row = 0, column = 0, pady = 10)

         #img orden produccion
        img_orden_produccion_path = resource_path("img/orden_de_produccion.png")
        img_orden_produccion = Image.open(img_orden_produccion_path)
        resized_img_orden = img_orden_produccion.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_image_orden = ImageTk.PhotoImage(resized_img_orden)

        #buton orden produccio
        self.button_orden_produccion = ctk.CTkButton(self.opciones_frame, image = self.converted_image_orden, text = "Orden de Producción",
                                                command = self.open_orden_de_produccion, state="disabled")
        self.button_orden_produccion.configure(width = 300, height = 50,
                                            fg_color ="#3B8ED0", text_color="black", hover_color="#36719F",
                                            border_width = 2,
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.button_orden_produccion.grid(row = 1, column = 0, pady = 10)

        #img control ordenes de compra
        img_ctrl_path = resource_path("img/produccion_programada.png")
        img_ctrl = Image.open(img_ctrl_path)
        resized_img_ctrl = img_ctrl.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_ctrl = ImageTk.PhotoImage(resized_img_ctrl)

        #buton producciones programadas
        self.button_prodprog = ctk.CTkButton(self.opciones_frame, image = self.converted_img_ctrl, text = "Producciones Programadas", 
                                         command = self.prod_programada, state="disabled")
        self.button_prodprog.configure(width = 300, height = 50,
                                            fg_color ="#3B8ED0", text_color="black", hover_color="#36719F",
                                            border_width = 2,
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.button_prodprog.grid(row = 2, column = 0, pady = 10)

        #img etiquetas
        img_etiquetas_path = resource_path("img/etiqueta.png")
        img_etiquetas = Image.open(img_etiquetas_path)
        resized_img_etiquetas = img_etiquetas.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_etiquetas = ImageTk.PhotoImage(resized_img_etiquetas)

        #buton ctrl ordenes de produccion
        self.button_ctrl_prod = ctk.CTkButton(self.opciones_frame, image = self.converted_img_etiquetas, text = "Ctrl. de Ordenes de Producción",
                                         command = self.ctrl_ordenes_prod,state="disabled")
        self.button_ctrl_prod.configure(width = 300, height = 50,
                                            fg_color ="#3B8ED0", text_color="black", hover_color="#36719F",
                                            border_width = 2,
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.button_ctrl_prod.grid(row = 3, column = 0, pady = 10)

        #img orden compra
        img_catalogo_path = resource_path("img/catalogo.png")
        img_catalogo = Image.open(img_catalogo_path)
        resized_img_catalogo = img_catalogo.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_catalogo = ImageTk.PhotoImage(resized_img_catalogo)

        #buton catalogo
        self.button_compra = ctk.CTkButton(self.opciones_frame, image = self.converted_img_catalogo, text = "Ctrl. de\nProductos Faltantes", state = "disabled",
                                           command = self.ctrl_orden_compra)
        self.button_compra.configure(width = 300, height = 50,
                                            fg_color ="#3B8ED0", text_color="black", hover_color="#36719F",
                                            border_width = 2,
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.button_compra.grid(row = 4, column = 0, pady = 10)

        #img utilerias
        img_utilerias_path = resource_path("img/utilerias.png")
        img_utilerias = Image.open(img_utilerias_path)
        resized_img_utilerias = img_utilerias.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_utilerias = ImageTk.PhotoImage(resized_img_utilerias)

        #buton utilerias
        self.button_utilerias = ctk.CTkButton(self.opciones_frame, image = self.converted_img_utilerias, text = "Utilerías del Sistema",
                                         state = "disabled", command = self.utilerias)
        self.button_utilerias.configure(width = 300, height = 50,
                                            fg_color ="#3B8ED0", text_color=("black","white"), hover_color="#36719F",
                                            border_width = 2,
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.button_utilerias.grid(row = 6, column = 0, pady = 10)

        #img utilerias
        img_coti_remi_web_path = resource_path("img/cotizaciones_web.png")
        img_coti_remi_web = Image.open(img_coti_remi_web_path)
        resized_img_coti_remi_web = img_coti_remi_web.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_coti_remi_web = ImageTk.PhotoImage(resized_img_coti_remi_web)

        #buton utilerias
        self.button_coti_remi_web = ctk.CTkButton(self.opciones_frame, image = self.converted_img_coti_remi_web, text = "Consulta Pedidos Web",
                                         state = "disabled", command=self.open_coti_remi_web)
        self.button_coti_remi_web.configure(width = 300, height = 50,
                                            fg_color ="#3B8ED0", text_color=("black","white"), hover_color="#36719F",
                                            border_width = 2,
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.button_coti_remi_web.grid(row = 5, column = 0, pady = 10)

        #criterio para habilitar botones
        if int(self.user_niv) == 1:
            self.button_consolidar_pedidos.configure(state = "normal")
            self.button_orden_produccion.configure(state = "normal")
            self.button_prodprog.configure(state = "normal")
            self.button_ctrl_prod.configure(state = "normal")
            self.button_utilerias.configure(state = "normal")
            self.button_compra.configure(state = "normal")
            self.button_coti_remi_web.configure(state = "normal")

        elif int(self.user_niv) == 2:
            self.button_consolidar_pedidos.configure(state = "normal")
            self.button_orden_produccion.configure(state = "normal")
            self.button_prodprog.configure(state = "normal")
            self.button_ctrl_prod.configure(state = "normal")
            self.button_compra.configure(state = "normal")
            # self.button_coti_remi_web.configure(state = "normal")

        elif int(self.user_niv) == 3:
            self.button_consolidar_pedidos.configure(state = "normal")
            self.button_orden_produccion.configure(state = "normal")
            self.button_coti_remi_web.configure(state = "normal")

        elif int(self.user_niv) == 4:
            self.button_prodprog.configure(state = "normal")
            self.button_ctrl_prod.configure(state = "normal")
            self.button_compra.configure(state = "normal")

    def open_consolidar_pedidos_window(self):
        hide_all_frames(self.master)
        niv_user = self.user_niv
        consolidarpedidos = consolidar_pedidos_window(self.master, niv_user)
        consolidarpedidos.pack(fill="both", expand=True)
    
        # open_consolidar_pedidos = consolidar_pedidos_window()
    
    def open_orden_de_produccion(self):
        hide_all_frames(self.master)
        niv_user = self.user_niv
        ordenproduc = orden_produccion_window(self.master, niv_user)
        ordenproduc.pack(fill="both", expand=True)

    def prod_programada(self):
        hide_all_frames(self.master)
        niv_user = self.user_niv
        prod_programada = prod_programada_window(self.master, niv_user)
        prod_programada.pack(fill="both", expand=True)

    def ctrl_ordenes_prod(self):
        hide_all_frames(self.master)
        niv_user = self.user_niv
        ctrl_orden_prod = ctrl_ordenes_prod_window(self.master, niv_user)
        ctrl_orden_prod.pack(fill="both", expand=True)
    
    def utilerias(self):
        hide_all_frames(self.master)
        niv_user = self.user_niv
        open_utilerias = utilerias(self.master, niv_user)
        open_utilerias.pack(fill="both", expand=True)

    def ctrl_orden_compra(self):
        hide_all_frames(self.master)
        niv_user = self.user_niv
        ctrl_orden_compra = ctrl_orden_compra_window(self.master, niv_user)
        ctrl_orden_compra.pack(fill="both", expand=True)

    def open_coti_remi_web(self):
        hide_all_frames(self.master)
        niv_user = self.user_niv
        coti_remi_web = coti_remi_web_window(self.master, niv_user)
        coti_remi_web.pack(fill="both", expand=True)

class consolidar_pedidos_window(tk.Frame):
    def __init__(self, master, niv_user):
        super().__init__(master)

        self.niv_user = niv_user

        self.columnconfigure((0,1,2,3,4), weight = 1)
        self.rowconfigure((0,1,2,3), weight = 1)
       

          #img descargar
        img_cloud_path = resource_path("img/cloud.png")
        img_cloud = Image.open(img_cloud_path)
        resized_img_cloud = img_cloud.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_cloud = ImageTk.PhotoImage(resized_img_cloud)

        self.folio_frame = tk.Frame(self)
        self.folio_frame.grid(row = 0, column = 0, padx = 10, pady = 10, sticky = "nw")

        self.campos_pedidos_frame = tk.Frame(self)
        self.campos_pedidos_frame.grid(row = 1, column = 0, sticky = "nw", padx = 10, pady = 10)

        self.bajo_frame = tk.Frame(self)
        self.bajo_frame.grid(row = 3, column = 0, sticky = "nw", padx = 10, pady = 10)

        self.campos_pedidos()

        self.tab_frame =tk.Frame(self)
        self.tab_frame.grid(row = 2, column = 0, sticky = "nw", padx = 10)
        notebook = ctk.CTkTabview(self.tab_frame, 
                                  width = 980, height = 400)
        notebook._segmented_button.configure(text_color = "black",
                                             font = ("Roboto", 16))
        notebook.grid(row = 0, column = 0, columnspan = 4, padx = 10, pady = 10, sticky = "nw")
        # productos = tk.Frame(notebook)
        # pedidos = tk.Frame(notebook)
        self.productos = notebook.add("Productos")
        self.pedidos = notebook.add("Pedidos")
       
        self.tabla_productos()
    def campos_pedidos(self):
        
        #folio label
        self.folio_label = tk.Label(self.folio_frame, text = "Folio")
        self.folio_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.folio_label.grid(row = 0, column = 0, sticky = "w")

        #entry folio
        self.my_folio = tk.StringVar()
        self.entry_folio = tk.Entry(self.folio_frame, textvariable = self.my_folio)
        self.entry_folio.configure(width = 15 , font = ("Roboto", 12))
        self.entry_folio.grid(row = 1, column = 0, sticky = "w")

        #boton buscar folio
        img_buscar_path = resource_path("img/lupa.png")
        img_buscar = Image.open(img_buscar_path)
        resized_img_buscar = img_buscar.resize((20,20), Image.Resampling.LANCZOS)
        self.converted_img_buscar = ImageTk.PhotoImage(resized_img_buscar)

        #buton buscar folio  
        self.boton_folio = ctk.CTkButton(self.folio_frame, text = "", image = self.converted_img_buscar, command = self.search_folio)
        self.boton_folio.configure(width = 25, height = 25,
                                   border_width = 2)
        self.boton_folio.grid(row = 1, column = 1, padx = 5, sticky = "w")

         #label fecha
        self.fecha_label = tk.Label(self.campos_pedidos_frame, text = "Fecha descarga de pedidos (dd/mm/aaaa)")
        self.fecha_label.configure(font = ("Roboto", 12, "bold"), anchor = "w")
        self.fecha_label.grid(row = 0, column = 0, columnspan = 5, sticky = "w")

        #entry fecha
        self.my_fecha = tk.StringVar()
        self.entry_fecha = tk.Entry(self.campos_pedidos_frame, textvariable = self.my_fecha)
        # self.entry_fecha.insert(tk.END, "  /  /    ")
        self.entry_fecha.config(width = 10 , font = ("Roboto", 12))
        self.entry_fecha.bind('<Key>', self.datemask)
        self.entry_fecha.bind('<BackSpace>', self.handle_backspace)
        self.entry_fecha.grid(row = 1, column = 0, columnspan = 3, sticky = "w")

        #label pedidos web
        self.archivos_web_label = tk.Label(self.campos_pedidos_frame, text = "Archivo Pedidos WEB")
        self.archivos_web_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.archivos_web_label.grid(row = 2, column = 0, sticky = "w", pady = 5)

        #entry pedidos web
        self.my_csv = tk.StringVar()
        self.entry_csv = tk.Entry(self.campos_pedidos_frame, textvariable = self.my_csv, state = "disabled")
        self.entry_csv.configure(width = 60, font = ("Roboto", 12))
        self.entry_csv.grid(row = 3, column = 0, columnspan = 10, sticky = "w")

        self.boton_csv= ctk.CTkButton(self.campos_pedidos_frame, text = "", image = self.converted_img_buscar, command=self.select_csv_web)
        self.boton_csv.configure(width = 25, height = 25,
                                   border_width = 2)
        self.boton_csv.grid(row = 3, column = 10, padx = 10)

        #boton home 
        #img home
        img_home_path = resource_path("img/home.png")
        img_home = Image.open(img_home_path)
        resized_img_home = img_home.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_home = ImageTk.PhotoImage(resized_img_home)

        #buton salir a home
        self.boton_salir = ctk.CTkButton(self.bajo_frame, image = self.converted_img_home, text = "Home", command = self.open_main_window)
        self.boton_salir.configure(width = 200, height = 30,
                                   fg_color ="#FF5050", text_color="black", hover_color="#DD1818",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_salir.grid(row = 0, column = 0)

        #buton descarga
        self.boton_descarga = ctk.CTkButton(self.folio_frame, image = self.converted_img_cloud, text = "Descarga\nPedidos", command = self.descagra_archivos)
        self.boton_descarga.configure(width = 50, height = 30,
                                   border_width = 2,
                                   font = ("Roboto", 14, "bold"),
                                   compound  = tk.TOP)
        self.boton_descarga.grid(row = 0, column = 1, rowspan = 2,padx = 700, pady =5)

    def descagra_archivos(self):
        try:
            #get folio value
            self.fecha = self.entry_fecha.get()
            self.fecha = self.fecha.replace("/", "-")
            print(self.fecha)

            #run the query to get the pedidos from SAE
            list_sae_pedidos = query_descargar_pedidos(self.fecha)
            list_all_pedidos = query_list_all_pedidos()

            #transform that list into a data frame
            df_sae_pedidos = pd.DataFrame(list_sae_pedidos, columns = ["nopedido", "cveprod", "skuvari", "desvari", "estatus", "stenvio", "fecha", "cant"])
            
            #add this new columns to sae pedidos
            df_sae_pedidos["origen"] = "SAE"
            df_sae_pedidos["fechaalta"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            df_sae_pedidos["desprod"] = ""

            #delete all the spaces in the columns
            df_sae_pedidos["nopedido"] = df_sae_pedidos['nopedido'].str.strip()

            # print("tipos de sae", df_sae_pedidos.dtypes)
            
            # read data from a csv file
            df_web_file = self.entry_csv.get()

            if df_web_file:
                df_web_file = pd.read_csv(df_web_file)
                
            #select the columns you want from the df_web_file
                # selected_columns = ["order_name", "product_title", "variant_sku", "variant_title", "financial_status", "hour","net_quantity"]
                selected_columns = ["Nombre del pedido", "Nombre del producto", "SKU de variante de producto al momento de la venta", 
                                    "Título de variante de producto al momento de la venta", 
                                    "Estado de pago del pedido", "Hora","Artículos netos vendidos"]
                df_web_file = df_web_file[selected_columns]
                df_web_file["cveprod"] = ""
                df_web_file["origen"] = "WEB"
                df_web_file["fechaalta"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
                
                
            # rename the columns of the data frame
            # Rename specific columns
                # df_web_file.rename(columns={'order_name': 'nopedido',
                #                         'product_title': 'desprod', 
                #                         'variant_sku': 'skuvari',
                #                         'variant_title': 'desvari',
                #                         'financial_status': 'estatus',
                #                         'hour': 'fecha',
                #                         'net_quantity': 'cant'}, inplace=True)
                df_web_file.rename(columns={'Nombre del pedido': 'nopedido',
                                        'Nombre del producto': 'desprod', 
                                        'SKU de variante de producto al momento de la venta': 'skuvari',
                                        'Título de variante de producto al momento de la venta': 'desvari',
                                        'Estado de pago del pedido': 'estatus',
                                        'Hora': 'fecha',
                                        'Artículos netos vendidos': 'cant'}, inplace=True)                        
                df_web_file = df_web_file.reindex(columns=["nopedido", "desprod", "cveprod", 
                                                           "skuvari", "desvari", "estatus", 
                                                           "stenvio", "fecha", "cant", "origen",
                                                           "fechaalta"])
                if df_web_file['skuvari'].isnull().any() or (df_web_file['skuvari'] == '').any():
                    messagebox.showerror("Error", "El archivo WEB no puede ser cargado debido a que contiene SKU de productos vacíos")
                else:            
                    df_web_file['stenvio'] = df_web_file['stenvio'].astype(str)
                    # df_web_file['skuvari'] = df_web_file['skuvari'].astype(str)
                    df_web_file['cant'] = df_web_file['cant'].astype(float)
                    
                    df_web_file['fecha'] = pd.to_datetime(df_web_file['fecha'], format='mixed', dayfirst=True)
                    print('fecha_2')
                
                #filter the data

                    df_web_file = df_web_file[df_web_file['cant'] >= 0]
                    df_web_file = df_web_file[df_web_file['estatus'] == "paid"]

                    desc_df = query_desc_web()
                    
                
                    # identify matching rows
                    merged_df = pd.merge(df_web_file, desc_df[['skuvari', 'desvari']], on='skuvari', how='left', suffixes=('', '_from_web'))

                    # Delete the 'other_column'
                    merged_df = merged_df.drop(columns=['desvari'])

                    # Rename the 'desvari' column to 'new_desvari'
                    merged_df = merged_df.rename(columns={'desvari_from_web': 'desvari'})

                #bind by columns df of web and sae
                    df_concat = pd.concat([merged_df, df_sae_pedidos], axis=0)


                    # file_name = resource_path(f"client/_webfile.csv")
                    # df_concat.to_csv(file_name) #borrado

                #extract the data frame of all the pedidos
                    df_all_pedidos = pd.DataFrame(list_all_pedidos, columns = ["nopedido", "cveprod", "desprod", "skuvari","desvari", "estatus", "stenvio", "fecha", "cant", 
                                                                        "origen", "fechaalta", "folio" ])
                    
                #delete spaces of no pedido
                    df_all_pedidos["nopedido"] = df_all_pedidos['nopedido'].str.strip()
                    

                #filter the no pedidos that are in sae and web but not in all pedidos
                    not_in_pedidos = df_concat[~df_concat['nopedido'].isin(df_all_pedidos['nopedido'])]
                    not_in_pedidos = not_in_pedidos[["nopedido", "cveprod","desprod", "skuvari","desvari", "estatus", "stenvio", "fecha", "cant", "origen",
                                                "fechaalta"]]
                    
                    
                # #calculate the max folio and add it 1
                    max_folio = df_all_pedidos["folio"].max()
                    max_folio = max_folio if pd.notnull(max_folio) else 0
                    not_in_pedidos["folio"] = max_folio + 1

                    not_in_pedidos["fecha"] = pd.to_datetime(not_in_pedidos["fecha"])
                    not_in_pedidos["fechaalta"] = pd.to_datetime(not_in_pedidos["fechaalta"])

                    lista_data =  [tuple(x) for x in not_in_pedidos.to_records(index=False)]
                    query_insert_new(lista_data)


            else:
                 # Handle the case where the CSV entry is None
                print("No CSV file provided. Continuing with the script...")
            #extract the data frame of all the pedidos
                df_all_pedidos = pd.DataFrame(list_all_pedidos, columns = ["nopedido", "cveprod", "desprod", "skuvari","desvari", "estatus", "stenvio", "fecha", "cant", 
                                                                       "origen", "fechaalta", "folio" ])
            
            #delete spaces of no pedido
                df_all_pedidos["nopedido"] = df_all_pedidos['nopedido'].str.strip()

             #filter the no pedidos that are in sae but not in all pedidos
                not_in_pedidos = df_sae_pedidos[~df_sae_pedidos['nopedido'].isin(df_all_pedidos['nopedido'])]
                not_in_pedidos = not_in_pedidos[["nopedido", "cveprod","desprod", "skuvari","desvari", "estatus", "stenvio", "fecha", "cant", "origen",
                                             "fechaalta"]]
            
            # #calculate the max folio and add it 1
                max_folio = df_all_pedidos["folio"].max()
                not_in_pedidos["folio"] = max_folio + 1

             #transform data to correct type
                not_in_pedidos["fecha"] = pd.to_datetime(not_in_pedidos["fecha"])
                not_in_pedidos["fechaalta"] = pd.to_datetime(not_in_pedidos["fechaalta"])

                lista_data =  [tuple(x) for x in not_in_pedidos.to_records(index=False)]
                query_insert_new(lista_data)

                print(not_in_pedidos)

            if len(not_in_pedidos) > 0:
                titulo = "Archivo cargado con Éxito"
                total_agg = len(not_in_pedidos)
                folio_agg = max_folio+1
                mensaje = mensaje = "Se agregaron '{}' productos a pedidos \n con el folio '{}'".format(total_agg, folio_agg)
                messagebox.showinfo(titulo, mensaje)

                self.tabla.delete(*self.tabla.get_children())
                self.lista_productos_tabla = lista_prod(max_folio+1)
                self.lista_productos_tabla.reverse()


                for p in self.lista_productos_tabla:
                    self.tabla.insert('', 0, text=p[0], values=(p[0:]))
            else:
                titulo = "Archivo cargado con Éxito"
                mensaje = "No se agregaron productos a pedidos \n se muestan los pedidos con el último folio agregado"
                messagebox.showinfo(titulo, mensaje)

                self.tabla.delete(*self.tabla.get_children())
                self.lista_productos_tabla = lista_prod(max_folio)
                self.lista_productos_tabla.reverse()


                for p in self.lista_productos_tabla:
                    self.tabla.insert('', 0, text=p[0], values=(p[0:]))
                
        except Exception as e:
            titulo = "Error"
            mensaje = f"No se pudo cargar el archivo:"
            messagebox.showerror(titulo, mensaje)
            print(e)

    def open_main_window(self):
        hide_all_frames(self.master)
        niv_user = self.niv_user
        opciones_window = opcioneswindow(self.master, niv_user)
        opciones_window.pack(fill="both", expand=True)
    
    def select_csv_web(self):
        filepath = filedialog.askopenfilename()
        self.entry_csv.config(state = "normal")
        self.entry_csv.delete(0, 'end')  # Clear the entry widget
        self.entry_csv.insert(0, filepath) 
        self.entry_csv.config(state =  "disabled")
 
    def tabla_productos(self):
     # Recover list of products for folio
        

     #create table to show the entrys
        self.tabla = ttk.Treeview(self.productos,
                                  column = ("Folio","SKU","Descripcion", "Cantidad", "Exist", "Min"),
                                  show = "headings")
         # Add headings
        self.tabla.heading("Folio", text="Folio")
        self.tabla.heading("SKU", text="SKU")
        self.tabla.heading("Descripcion", text="Descripción")
        self.tabla.heading("Cantidad", text="Cantidad")
        self.tabla.heading("Exist", text="Exist")
        self.tabla.heading("Min", text="Min")

    # Configure column width
        self.tabla.column("Folio", width=50)
        self.tabla.column("SKU", width=50) 
        self.tabla.column("Descripcion", width=200) 
        self.tabla.column("Cantidad", width=50)
        self.tabla.column("Exist", width=50)  
        self.tabla.column("Min", width=50)   # Adjust the width as needed
       
        
    
    # Pack the treeview inside the productos tab frame
        self.tabla.pack(fill=tk.BOTH, expand=True,padx=10, pady=10)

    # Bind the selection event to a handler
        self.tabla.bind("<<TreeviewSelect>>", self.on_tabla_productos_selected)
    
    #create the tabla for pedidos
        self.tabla_2 = ttk.Treeview(self.pedidos,
                                  column = ("No. Pedido", "SKU", "Descripcion", "Cantidad", "Origen"),
                                  show = "headings")
         # Add headings
        self.tabla_2.heading("No. Pedido", text="No. Pedido")
        self.tabla_2.heading("SKU", text="SKU")
        self.tabla_2.heading("Descripcion", text="Descripción")
        self.tabla_2.heading("Cantidad", text="Cantidad")
        self.tabla_2.heading("Origen", text="Origen")

        # Configure column width
        self.tabla_2.column("No. Pedido", width=50)  # Adjust the width as needed
        self.tabla_2.column("SKU", width=50)
        self.tabla_2.column("Descripcion", width=250)
        self.tabla_2.column("Cantidad", width=50)
        self.tabla_2.column("Origen", width=50)

    #pack the table for pedidos
        self.tabla_2.pack(fill=tk.BOTH, expand=True,padx=10, pady=10)
    
    def search_folio(self):
        self.folio_valor = self.my_folio.get()

        self.tabla.delete(*self.tabla.get_children())
        self.lista_productos_tabla = lista_prod(self.folio_valor)
        self.lista_productos_tabla.reverse()


        for p in self.lista_productos_tabla:
            self.tabla.insert('', 0, text=p[0], values=(p[0:]))

    def on_tabla_productos_selected(self, event):
        
        selected_item = self.tabla.selection()[0]
        item_values= self.tabla.item(selected_item, "values")
        self.sku_value = f"{item_values[1]}"
        self.folio_select = f"{item_values[0]}"
        

    # # Clear the TreeView pedidos
        for i in self.tabla_2.get_children():
            self.tabla_2.delete(i)

        self.lista_pedidos_tabla = lista_pedidos(self.sku_value, self.folio_select)

        for k in self.lista_pedidos_tabla:
            self.tabla_2.insert('', 0, text=k[0], values=(k[0:]))
        
    def datemask(self, event):
        char = event.char
        if not char.isdigit() and char != '\x08':  # Allow only digits and backspace
            return 'break'
        
        current_text = self.my_fecha.get()
        if len(current_text) == 10:
            return 'break'  # Stop further input if date is complete
        
        if char.isdigit():
            if len(current_text) in [2, 5]:
                self.my_fecha.set(current_text + '/' + char)
            else:
                self.my_fecha.set(current_text + char)
            return 'break'

    def handle_backspace(self, event):
        current_text = self.my_fecha.get()
        if current_text:
            if current_text[-1] == '/':
                self.my_fecha.set(current_text[:-1])
            self.my_fecha.set(current_text[:-1])

class orden_produccion_window(tk.Frame):
    def __init__(self, master, niv_user):
        super().__init__(master)

        self.niv_user = niv_user

        self.columnconfigure((0,1,2,3,4,5), weight = 1)
        # self.rowconfigure((0,1,2,3), weight = 1)

        self.folio_frame = tk.Frame(self)
        self.folio_frame.grid(row = 0, column = 0, padx = 10, pady = 10, sticky = "nw")

        self.tabla_frame = tk.Frame(self)
        self.tabla_frame.grid(row =1, column = 0)
        self.botones_frame = tk.Frame(self)
        self.botones_frame.grid(row = 1, column = 5, padx = 10, pady = 10, sticky = "nw")

        self.campos_produccion()
        self.tabla_produccion()

    def campos_produccion(self):
        #folio label
        self.folio_label = tk.Label(self.folio_frame, text = "Folio")
        self.folio_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.folio_label.grid(row = 0, column = 0, sticky = "w")

        #entry folio
        self.my_folio = tk.StringVar()
        self.entry_folio = tk.Entry(self.folio_frame, textvariable = self.my_folio)
        self.entry_folio.configure(width = 15 , font = ("Roboto", 12))
        self.entry_folio.grid(row = 0, column = 1, sticky = "w")

        #boton buscar folio
        img_buscar_path = resource_path("img/lupa.png")
        img_buscar = Image.open(img_buscar_path)
        resized_img_buscar = img_buscar.resize((20,20), Image.Resampling.LANCZOS)
        self.converted_img_buscar = ImageTk.PhotoImage(resized_img_buscar)

        #buton buscar folio  
        self.boton_folio = ctk.CTkButton(self.folio_frame, text = "", image = self.converted_img_buscar, command = self.search_folio_produccion)
        self.boton_folio.configure(width = 25, height = 25,
                                   border_width = 2)
        self.boton_folio.grid(row = 0, column = 2, padx = 5, sticky = "w")

        #boton surtir
        img_surtir_path = resource_path("img/surtir.png")
        img_surtir = Image.open(img_surtir_path)
        resized_img_surtir = img_surtir.resize((50,50), Image.Resampling.LANCZOS)
        self.converted_img_surtir = ImageTk.PhotoImage(resized_img_surtir)

        self.boton_surtir = ctk.CTkButton(self.botones_frame, text = "A surtir", image =self.converted_img_surtir, command = self.a_surtir )
        self.boton_surtir.configure(width = 200, height = 50,
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_surtir.grid(row = 0, column = 0, padx = 10, pady = 10)

        #boton produccion
        img_produccion_path = resource_path("img/orden_de_produccion.png")
        img_produccion = Image.open(img_produccion_path)
        resized_img_produccion = img_produccion.resize((50,50), Image.Resampling.LANCZOS)
        self.converted_img_produccion = ImageTk.PhotoImage(resized_img_produccion)

        self.boton_produccion= ctk.CTkButton(self.botones_frame, text = "Orden de \n Producción", image =self.converted_img_produccion, command = self.orden_prod)
        self.boton_produccion.configure(width = 200, height = 50,
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_produccion.grid(row = 1, column = 0, pady = 5, padx = 10)

         #boton archivo pedidos
        img_archivo_path = resource_path("img/archivo.png")
        img_archivo = Image.open(img_archivo_path)
        resized_img_archivo = img_archivo.resize((40,40), Image.Resampling.LANCZOS)
        self.converted_img_archivo = ImageTk.PhotoImage(resized_img_archivo)

        self.boton_archivo= ctk.CTkButton(self.botones_frame, text = "Crear Cotizaciones \n y\n Archivo Pedidos WEB", image =self.converted_img_archivo, command = self.archivo_ped)
        self.boton_archivo.configure(width = 200, height = 50,
                                   border_width = 2,
                                   font = ("Roboto", 14, "bold"),
                                   compound  = tk.LEFT)
        self.boton_archivo.grid(row = 2, column = 0, pady = 5, padx = 10)

         #boton A comprar
        img_comprar_path = resource_path("img/comprar.png")
        img_comprar = Image.open(img_comprar_path)
        resized_img_comprar = img_comprar.resize((50,50), Image.Resampling.LANCZOS)
        self.converted_img_comprar = ImageTk.PhotoImage(resized_img_comprar)

        self.boton_comprar = ctk.CTkButton(self.botones_frame, text = "A Comprar", image =self.converted_img_comprar, command = self.a_comprar)
        self.boton_comprar.configure(width = 200, height = 50,
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_comprar.grid(row = 3, column = 0, pady = 5, padx =10)

    #boton home 
    #img home
        img_home_path = resource_path("img/home.png")
        img_home = Image.open(img_home_path)
        resized_img_home = img_home.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_home = ImageTk.PhotoImage(resized_img_home)

        #buton salir a home
        self.boton_salir = ctk.CTkButton(self.botones_frame, image = self.converted_img_home, text = "Home", command = self.open_main_window)
        self.boton_salir.configure(width = 200, height = 30,
                                   fg_color ="#FF5050", text_color="black", hover_color="#DD1818",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_salir.grid(row = 4, column = 0, pady = 10)
    
    def tabla_produccion(self):
     #create table to show the entrys
        self.tabla = ttk.Treeview(self.tabla_frame,
                                  column = ("OP","Folio","Cantidad","SKU", "Descripcion", "Lote", "Consumo"),
                                  height = 30,
                                  show =  "headings"
                                  )
        # Add headings
        self.tabla.heading("OP", text="O.P")
        self.tabla.heading("Folio", text="Folio")
        self.tabla.heading("Cantidad", text="Cantidad")
        self.tabla.heading("SKU", text="SKU")
        self.tabla.heading("Descripcion", text="Descripción")
        self.tabla.heading("Lote", text="Lote")
        self.tabla.heading("Consumo", text="Consumo")

    # Configure column width
        self.tabla.column("OP", width=50)
        self.tabla.column("Folio", width=50)
        self.tabla.column("Cantidad", width=100)
        self.tabla.column("SKU", width=75)
        self.tabla.column("Descripcion", width=250) 
        self.tabla.column("Lote", width=75)    
        self.tabla.column("Consumo", width=100) 

    
    # Pack the treeview inside the productos tab frame
        self.tabla.grid(row = 0, column = 0, columnspan = 7, padx=5, pady=5, sticky = "ew")

        #add scrollbar
        self.scroll = ttk.Scrollbar(self.tabla_frame,
                                    orient = "vertical", 
                                    command = self.tabla.yview)
        self.scroll.grid(row = 0, column = 7, sticky = "nse")
        self.tabla.configure(yscrollcommand = self.scroll.set)

    def search_folio_produccion(self):
        self.folio_produccion = self.my_folio.get()
       
        
        self.tabla.delete(*self.tabla.get_children())
        self.lista_produccion_tabla = query_busqueda_op(self.folio_produccion)
        self.lista_produccion_tabla.reverse()


        for p in self.lista_produccion_tabla:
            self.tabla.insert('', 0, text=p[0], values=(p[0:]))

    def a_surtir(self):
        try:
            #get folio value
            self.folio_surtir = self.my_folio.get()
            #run the query
            list_surtir = query_asurtir(self.folio_surtir)

            #export that list into a csv file
            df_surtir = pd.DataFrame(list_surtir, columns = ["Folio", "Origen", "SKU", "Descripcion", "Cantidad", "Stock Min"])
            df_surtir.set_index("Folio", inplace=True)

            file_name = resource_path(f"client/aSurtir_00{self.folio_surtir}.csv")
            df_surtir.to_csv(file_name)


            #box of success
            titulo = "Archivo Generado con Éxito"
            mensaje = f"Archivo _internal/client/aSurtir_00 {self.folio_surtir}.csv generado"
            messagebox.showinfo(titulo, mensaje)
        except Exception as e:
            titulo = "Error"
            mensaje = f"No se pudo generar el archivo: {str(e)}"
            messagebox.showerror(titulo, mensaje)

    def orden_prod(self):
        try:
            #get folio value
            self.folio_op = self.my_folio.get()
            #run the query
            list_op = query_op(self.folio_op)

            #export that list into a csv file
            df_op = pd.DataFrame(list_op, columns = ["O.P.", "Cantidad", "SKU", "Descripcion", "Lote", "Consumo", "Folio"])
            df_op.set_index("O.P.", inplace=True)
            
            file_name = resource_path(f"client/OP_00{self.folio_op}.csv")
            df_op.to_csv(file_name)

            #box of success
            titulo = "Archivo Generado con Éxito"
            mensaje = f"Archivo _internal/client/OP_00 {self.folio_op}.csv generado"
            messagebox.showinfo(titulo, mensaje)
        except Exception as e:
            titulo = "Error"
            mensaje = f"No se pudo generar el archivo: {str(e)}"
            messagebox.showerror(titulo, mensaje)
        
    def archivo_ped(self):
        try:
            #get folio value
            self.folio_archivo_ped = self.my_folio.get()
            #run the query
            list_archivo_pedidos = query_archivo_pedidos(self.folio_archivo_ped)

            #export that list into a csv file
            # df_ap = pd.DataFrame(list_archivo_pedidos, columns = ["Documento", "Cantidad", "Clave", "Costo",  "Concepto", "Fecha", "Almacen"])
            df_ap = pd.DataFrame(list_archivo_pedidos, columns = ["Documento", "Cliente", "Clave", "Cantidad",  "SKUVARI", "Precio", "Costo", "Almacen" ])
            # df_ap.set_index("Documento", inplace=True)
            df_ap['folio'] = self.folio_archivo_ped
            df_ap['Fecha'] = datetime.now().strftime("%Y-%m-%d")


            df_ap = df_ap[["folio","Documento", "Cliente", "Clave", "Cantidad",  "SKUVARI", "Precio", "Costo", "Fecha", "Almacen" ]]
            # df_ap.set_index("folio", inplace=True)
            
            file_name = resource_path(f"client/PEDWEB_00{self.folio_archivo_ped}.csv")
            df_ap.to_csv(file_name)

            df_ap['band'] = 'SA'

            lista_ped_web = df_ap.values.tolist()
            insert_into_pedweb(self.folio_archivo_ped, lista_ped_web)

            #box of success
            titulo = "Archivo Generado con Éxito"
            mensaje =  f"Archivo _internal/client/PEDWEB_00 {self.folio_archivo_ped}.csv generado"
            messagebox.showinfo(titulo, mensaje)


            #proceso para cotizaciones
            try:
                x = select_ped_web(self.my_folio.get())
                df = pd.DataFrame(x, columns = ["folio","tot_partidas", "documento", "cliente","clave_pedido","cant_total", "fecha", "almacen", "estatus"])
                if df['estatus'].iloc[0] == 'SA':
                    for i, row in df.iterrows():
                        partidas_pedido = select_detalle(row['documento'])
                        df_partidas_pedido = pd.DataFrame(partidas_pedido, columns = ["folio", "documento", "skuvari", "cantidad", "precio","costo"])
                        cve_doc = max_cvedoc_factc()
                        df_gral = pd.DataFrame()
                        cve_bita = select_cvebita()
                        
                        for i, partidas in df_partidas_pedido.iterrows():
                            impuestos_df = impuestos_producto(str(partidas['skuvari']))
                            contador = i+1
                            partida_df = par_factc(cve_doc,contador, partidas, impuestos_df)
                            df_gral =  pd.concat([df_gral,partida_df], ignore_index=True)
                            parfactc_clib(cve_doc, contador)


                        can_tot, total_impuestos = general_factc(cve_doc, row, df_gral, cve_bita)
                        insert_bita(cve_bita, cve_doc, can_tot)
                        afacta(can_tot, total_impuestos)
                        fact_clib(cve_doc)
                        update_folios_fac(cve_doc)
                        update_tab_ctrl()



                    update_pedweb(self.my_folio.get())

                    messagebox.showinfo("Cotizaciones creadas", f"Se agregaron {len(df)} cotizaciones")
                else:
                    messagebox.showwarning("Aviso!", f"No se generaron nuevas cotizaciones debido a que el  folio {self.my_folio.get()} ya ha sido agregado previamente.")

            except Exception as e:
                titulo = "Error"
                mensaje = f"Error al crear cotizaciones: {str(e)}"
                messagebox.showerror(titulo, mensaje)
                error_df = pd.read_csv(resource_path(f"conexion/error.csv"))
                new_row = {"error": str(e), "date": datetime.now().strftime(("%Y-%m-%d %H:%M:%S"))}
                error_df.loc[len(error_df)] = new_row
                print(error_df)
                error_df.to_csv(resource_path(f"conexion/error.csv"), index=False)
            
        except Exception as e:
            titulo = "Error"
            mensaje = f"No se pudo generar el archivo: {str(e)}"
            messagebox.showerror(titulo, mensaje)
            print(e)
        
    def a_comprar(self):
        try:
            #get folio value
            self.folio_acomprar = self.my_folio.get()
            #run the query
            list_acomprar = query_acomprar(self.folio_acomprar)

            #export that list into a csv file
            df_acomprar = pd.DataFrame(list_acomprar, columns = ["Folio", "SKU", "Descripcion", "Cant. a Comprar", "Existencia", "STOCK MIN"])
            df_acomprar.set_index("Folio", inplace=True)
            
            file_name = resource_path(f"client/COMP_00{self.folio_acomprar}.csv")
            df_acomprar.to_csv(file_name)

            #box of success
            titulo = "Archivo Generado con Éxito"
            mensaje =  f"Archivo _internal/client/COMP_00 {self.folio_acomprar}.csv generado"
            messagebox.showinfo(titulo, mensaje)
        except Exception as e:
            titulo = "Error"
            mensaje = f"No se pudo generar el archivo: {str(e)}"
            messagebox.showerror(titulo, mensaje)

    def open_main_window(self):
        hide_all_frames(self.master)
        niv_user = self.niv_user
        opciones_window = opcioneswindow(self.master, niv_user)
        opciones_window.pack(fill="both", expand=True)

class prod_programada_window(tk.Frame):
    def __init__(self, master, niv_user):
        super().__init__(master)

        self.niv_user = niv_user

        self.columnconfigure((0,1,2,3,4,5,6), weight = 1)
        # self.rowconfigure((0,1,2,3), weight = 1)

        self.campos_frame = tk.Frame(self)
        self.campos_frame.grid(row = 0, column = 0, padx = 10, pady = 10, sticky = "nw")

        self.tabla_frame = tk.Frame(self)
        self.tabla_frame.grid(row =1, column = 0, padx = 30, pady = 10, sticky="w")

        self.botones_frame = tk.Frame(self)
        self.botones_frame.grid(row = 2, column = 0, padx = 10, pady = 10, sticky = "nw")

        self.campos()
        self.tabla_prod_progrmada()

    def campos(self):
    ##### campos entrada
        #lote
        self.lote_label = tk.Label(self.campos_frame, text = "Lote")
        self.lote_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.lote_label.grid(row = 0, column = 0, padx = 10, pady = 10, sticky = "w")

        #entry lote
        self.my_lote = tk.StringVar()
        self.entry_lote = tk.Entry(self.campos_frame, textvariable = self.my_lote)
        self.entry_lote.configure(width = 15 , font = ("Roboto", 12))
        self.entry_lote.grid(row = 0, column = 1, padx = 10, pady = 10, sticky = "w")

        #consumo
        self.consumo_label = tk.Label(self.campos_frame, text = "Consumo")
        self.consumo_label.configure(font =  ("Roboto", 12, "bold"), anchor = "e")
        self.consumo_label.grid(row = 0, column = 2, padx=10, pady=10, sticky = "w" )

        #entry consumo
        self.my_consumo = tk.StringVar()
        self.entry_consumo = tk.Entry(self.campos_frame, textvariable = self.my_consumo)
        self.entry_consumo.configure(width = 15 , font = ("Roboto", 12))
        self.entry_consumo.grid(row = 0, column = 3, padx = 10, pady = 10, sticky = "w")

        #sku
        self.sku_label = tk.Label(self.campos_frame, text = "SKU")
        self.sku_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.sku_label.grid(row = 0, column = 4, padx = 10, pady = 10, sticky = "w")

        #entry sku
        self.my_sku = tk.StringVar()
        self.entry_sku = tk.Entry(self.campos_frame, textvariable = self.my_sku)
        self.entry_sku.configure(width = 15 , font = ("Roboto", 12))
        self.entry_sku.grid(row = 0, column = 5, padx = 10, pady = 10, sticky = "w")

        #cantidad
        self.cant_label = tk.Label(self.campos_frame, text = "Cantidad")
        self.cant_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.cant_label.grid(row = 1, column = 0, padx = 10, pady = 10, sticky = "w")

        #entry sku
        self.my_cant = tk.StringVar()
        self.entry_cant = tk.Entry(self.campos_frame, textvariable = self.my_cant)
        self.entry_cant.configure(width = 15 , font = ("Roboto", 12))
        self.entry_cant.grid(row = 1, column = 1, padx = 10, pady = 10, sticky = "w")

        #boton agregar producto
        img_buscar_path = resource_path("img/add_product.png")
        img_buscar = Image.open(img_buscar_path)
        resized_img_buscar = img_buscar.resize((30,30), Image.Resampling.LANCZOS)
        self.converted_img_buscar = ImageTk.PhotoImage(resized_img_buscar)

        #buton agregar_producto 
        self.boton_add = ctk.CTkButton(self.campos_frame, text = "Agregar a Nueva Orden", image = self.converted_img_buscar, command = self.agregar_productos)
        self.boton_add.configure(width = 100, height = 30,
                                   border_width = 2, text_color="black", hover_color="#36719F",
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.boton_add.grid(row = 0, column = 6, padx = 10, pady = 10, sticky = "w")

    
    #Botones de guardar los registros
            #boton buscar guardar registros en OP
        img_guardar = resource_path("img/archivo.png")
        img_guardar = Image.open(img_guardar)
        resized_img_guardar = img_guardar.resize((30,30), Image.Resampling.LANCZOS)
        self.converted_img_guardar = ImageTk.PhotoImage(resized_img_guardar)

        #buton guardar
        self.boton_guardar = ctk.CTkButton(self.botones_frame, text = "Guardar O.P", image = self.converted_img_guardar, command=self.guardar_op)
        self.boton_guardar.configure(width = 100, height = 30,
                                   border_width = 2, text_color="black", hover_color="#36719F",
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.boton_guardar.grid(row = 0, column = 0, padx = 10, pady = 10, sticky = "w")
        
        #boton eliminar
        img_eliminar = resource_path("img/trash.png")
        img_eliminar = Image.open(img_eliminar)
        resized_img_eliminar = img_eliminar.resize((30,30), Image.Resampling.LANCZOS)
        self.converted_img_eliminar = ImageTk.PhotoImage(resized_img_eliminar)

        #buton guardar
        self.boton_eliminar = ctk.CTkButton(self.botones_frame, text = "Eliminar", image = self.converted_img_eliminar, command=self.eliminar_pp)
        self.boton_eliminar.configure(width = 100, height = 30,
                                   border_width = 2, text_color="black", hover_color="#36719F",
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.boton_eliminar.grid(row = 0, column = 1, padx = 10, pady = 10, sticky = "w")


        #boton home 
    #img home
        img_home_path = resource_path("img/home.png")
        img_home = Image.open(img_home_path)
        resized_img_home = img_home.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_home = ImageTk.PhotoImage(resized_img_home)

        #buton salir a home
        self.boton_salir = ctk.CTkButton(self.botones_frame, image = self.converted_img_home, text = "Home", command = self.open_main_window)
        self.boton_salir.configure(width = 200, height = 30,
                                   fg_color ="#FF5050", text_color="black", hover_color="#DD1818",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_salir.grid(row = 0, column = 2, pady = 10)

    def tabla_prod_progrmada(self):
        self.lista_pp = listar_pp()

        #create table to show the entrys
        self.tabla = ttk.Treeview(self.tabla_frame,
                                  column = ("Lote","Consumo", "SKU","Cantidad", "DESC"),
                                  height = 20,
                                  show =  "headings",
                                  selectmode="extended"
                                  )
        
        # Add headings
        self.tabla.heading("Lote", text="Lote")
        self.tabla.heading("Consumo", text="Consumo")
        self.tabla.heading("SKU", text="SKU")
        self.tabla.heading("DESC", text = "Descripción")
        self.tabla.heading("Cantidad", text = "Cantidad")

        # Configure column width
        self.tabla.column("Lote", width=200)
        self.tabla.column("Consumo", width=200)
        self.tabla.column("SKU", width = 200)
        self.tabla.column("DESC", width = 275)
        self.tabla.column("Cantidad", width = 175)

        # Pack the treeview inside the productos tab frame
        self.tabla.grid(row = 0, column = 0,sticky="w")

      #iteration over lista e producciones programadas
        for p in self.lista_pp:
            self.tabla.insert('', 0, text=p[0], values=(p[0:]))

    def agregar_productos(self):
        lista_sku = inve_program()
        cve_art = self.my_sku.get()
        if cve_art not in lista_sku:
            messagebox.showerror("ERROR!", "El producto no existe, ingresar un producto diferente")
        else:
            fecha_obj = self.my_consumo.get()
            fecha_obj = datetime.strptime(fecha_obj, "%m/%Y")
            fecha_hoy = datetime.now()

            dos_anos = timedelta(days=2*365)

            if fecha_obj - fecha_hoy > dos_anos:
                print(fecha_obj-fecha_hoy)
                messagebox.showerror("ERROR!", "Fecha de consumo debe ser menor a 2 años")
            else:
        
                desvari = des_product(cve_art)
                lista_productos = [(self.my_lote.get(), self.my_consumo.get(), self.my_sku.get(), self.my_cant.get(), desvari)]

                #agregar productos a tabla de pp
                agregar_productos_pp(lista_productos)

                #actualiza la tabla de pp
                self.tabla_prod_progrmada()

                #limpia los campos
                self.my_lote.set("")
                self.my_consumo.set("")
                self.my_sku.set("")
                self.my_cant.set("")

    def open_main_window(self):
        hide_all_frames(self.master)
        niv_user = self.niv_user
        opciones_window = opcioneswindow(self.master, niv_user)
        opciones_window.pack(fill="both", expand=True)

    def guardar_op(self):
        seleccionar_pp()

        #actualiza la tabla
        self.tabla_prod_progrmada()
    def eliminar_pp(self):
        try:
            self.sku_pp = self.tabla.item(self.tabla.selection())['values'][2]
            eliminar_pp(self.sku_pp)

            self.tabla_prod_progrmada()
  
            self.sku_pp = None

            messagebox.showinfo("Eliminar Registro", "Se elimino el registro con éxito")
        except Exception as e:
            titulo = "Error"
            mensaje = f"No se pudo eliminar el registro: {str(e)}"
            messagebox.showerror(titulo, mensaje)

class ctrl_ordenes_prod_window(tk.Frame):
    def __init__(self, master, niv_user):
        super().__init__(master)

        self.niv_user = niv_user

        self.columnconfigure((0,1,2,3,4,5), weight = 1)
        # self.rowconfigure((0,1,2,3), weight = 1)

        self.folio_frame = tk.Frame(self)
        self.folio_frame.grid(row = 0, column = 0, padx = 10, pady = 10, sticky = "nw")

        self.tabla_frame = tk.Frame(self)
        self.tabla_frame.grid(row =1, column = 0)
        self.botones_frame = tk.Frame(self)
        self.botones_frame.grid(row = 1, column = 5, padx = 10, pady = 10, sticky = "nw")

        self.campos()
        self.tabla_ctrl_produccion()
    
    def campos(self):
        #folio label
        self.folio_label = tk.Label(self.folio_frame, text = "O.P")
        self.folio_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.folio_label.grid(row = 0, column = 0, sticky = "w")

        #entry folio
        self.my_folio = tk.StringVar()
        self.entry_folio = tk.Entry(self.folio_frame, textvariable = self.my_folio)
        self.entry_folio.configure(width = 15 , font = ("Roboto", 12))
        self.entry_folio.grid(row = 0, column = 1, sticky = "w")

        #boton buscar folio
        img_buscar_path = resource_path("img/lupa.png")
        img_buscar = Image.open(img_buscar_path)
        resized_img_buscar = img_buscar.resize((20,20), Image.Resampling.LANCZOS)
        self.converted_img_buscar = ImageTk.PhotoImage(resized_img_buscar)

        #buton buscar folio  
        self.boton_folio = ctk.CTkButton(self.folio_frame, text = "", image = self.converted_img_buscar, command = self.search_pendientes)
        self.boton_folio.configure(width = 25, height = 25,
                                   border_width = 2)
        self.boton_folio.grid(row = 0, column = 2, padx = 5, sticky = "w")

        #boton home 
    #img home
        img_home_path = resource_path("img/home.png")
        img_home = Image.open(img_home_path)
        resized_img_home = img_home.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_home = ImageTk.PhotoImage(resized_img_home)

        #buton salir a home
        self.boton_salir = ctk.CTkButton(self.botones_frame, image = self.converted_img_home, text = "Home", command = self.open_main_window)
        self.boton_salir.configure(width = 200, height = 30,
                                   fg_color ="#FF5050", text_color="black", hover_color="#DD1818",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_salir.grid(row = 4, column = 0, pady = 10)

        #boton surtir
        img_surtir_path = resource_path("img/surtir.png")
        img_surtir = Image.open(img_surtir_path)
        resized_img_surtir = img_surtir.resize((50,50), Image.Resampling.LANCZOS)
        self.converted_img_surtir = ImageTk.PhotoImage(resized_img_surtir)

        self.boton_surtir = ctk.CTkButton(self.botones_frame, text = "A surtir", image =self.converted_img_surtir, command=self.a_surtir)
        self.boton_surtir.configure(width = 200, height = 50,
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_surtir.grid(row = 0, column = 0, padx = 10, pady = 10)

        #boton fecha consumo
        img_consumo_path = resource_path("img/fecha.png")
        img_consumo = Image.open(img_consumo_path)
        resized_img_consumo = img_consumo.resize((50,50), Image.Resampling.LANCZOS)
        self.converted_img_consumo = ImageTk.PhotoImage(resized_img_consumo)

        self.boton_consumo = ctk.CTkButton(self.botones_frame, text = "Modificar \n Consumo",
                                           image =self.converted_img_consumo, command = self.open_modificar_consumo)
        self.boton_consumo.configure(width = 200, height = 50,
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_consumo.grid(row = 1, column = 0, padx = 10, pady = 10)

        #boton lote
        img_lote_path = resource_path("img/caja.png")
        img_lote = Image.open(img_lote_path)
        resized_img_lote = img_lote.resize((50,50), Image.Resampling.LANCZOS)
        self.converted_img_lote = ImageTk.PhotoImage(resized_img_lote)

        self.boton_lote = ctk.CTkButton(self.botones_frame, text = "Modificar \n Lote",
                                           image =self.converted_img_lote, command = self.open_modificar_lote)
        self.boton_lote.configure(width = 200, height = 50,
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_lote.grid(row = 2, column = 0, padx = 10, pady = 10)

    def tabla_ctrl_produccion(self):
     #create table to show the entrys
        self.tabla = ttk.Treeview(self.tabla_frame,
                                  column = ("OP","SKU", "Cantidad", "Descripcion", "lote", "consumo", "folio", "N_S", "receta", "tipo"),
                                  height = 30,
                                  show =  "headings",
                                  selectmode="extended"
                                  )
        
        # Add headings
        self.tabla.heading("OP", text="OP")
        self.tabla.heading("SKU", text="SKU")
        self.tabla.heading("Cantidad", text="Cantidad")
        self.tabla.heading("Descripcion", text="Descripción")
        self.tabla.heading("lote", text="Lote")
        self.tabla.heading("consumo", text="Consumo")
        self.tabla.heading("folio", text="Folio")
        self.tabla.heading("N_S", text="Estado de Producción")
        self.tabla.heading("receta", text="Receta")
        self.tabla.heading("tipo", text="Tipo")


    # Configure column width
        self.tabla.column("OP", width=50)
        self.tabla.column("SKU", width=80)
        self.tabla.column("Cantidad", width=75)
        self.tabla.column("Descripcion", width = 250)
        self.tabla.column("lote", width=50) 
        self.tabla.column("consumo", width=75)    
        self.tabla.column("folio", width=50) 
        self.tabla.column("N_S", width=125)
        self.tabla.column("receta", width=125)
        self.tabla.column("tipo", width=125)

        self.tabla.tag_configure("pass", background = "#ACF5A7") #surtido
        self.tabla.tag_configure("Por_producir", background = "#FF5247") #no surtido

    # Pack the treeview inside the productos tab frame
        self.tabla.grid(row = 0, column = 0, columnspan = 9, padx=5, pady=5, sticky = "ew")

        #add scrollbar
        self.scroll = ttk.Scrollbar(self.tabla_frame,
                                    orient = "vertical", 
                                    command = self.tabla.yview)
        self.scroll.grid(row = 0, column = 9, sticky = "nse")
        self.tabla.configure(yscrollcommand = self.scroll.set)


        self.folio_ctrl_op = self.my_folio.get()
        self.lista_ctrl_op_tabla = query_ctrl_orden_produccion(self.folio_ctrl_op)
        self.lista_ctrl_op_tabla.reverse()
        my_tag = "normal" #default tag

        for p in self.lista_ctrl_op_tabla:
            my_tag = "pass" if p[7] != "Por_producir" else "Por_producir"
            self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))
    
    def search_pendientes(self):

        self.folio_ctrl_op = self.my_folio.get()
        if self.folio_ctrl_op ==  "":
            self.tabla.delete(*self.tabla.get_children())
            self.lista_ctrl_op_tabla = query_ctrl_orden_produccion(self.folio_ctrl_op)
            self.lista_ctrl_op_tabla.reverse()

            for p in self.lista_ctrl_op_tabla:
                my_tag = "pass" if p[7] != "Por_producir" else "Por_producir"
                self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))
        else:
            self.tabla.delete(*self.tabla.get_children())
            self.lista_ctrl_op_tabla = query_ctrl_orden_produccion(self.folio_ctrl_op)
            self.lista_ctrl_op_tabla.reverse()

            for p in self.lista_ctrl_op_tabla:
                my_tag = "pass" if p[7] != "Por_producir" else "Por_producir"
                self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))

    def a_surtir(self):
        selected_items = self.tabla.selection()
        items_df = []
        for item in selected_items:
            current_item = self.tabla.item(item)['values']
            items_df.append(current_item)
        items_df = pd.DataFrame(items_df, columns=["OP", "SKU", "CANT", "DES", "LOTE", "CONSUMO", "FOLIO", "ST. PRODUCCION", "receta", "tipo"])
        items_df["SKU"] = items_df["SKU"].str.strip()

        materia_df = pd.DataFrame(columns = ["CVE_ART", "CVE_PROD", "DESCR", "CantNec", "EXIST", "STOCK_MIN"])
        if (items_df['ST. PRODUCCION'] == 'Producido').any(): 
            messagebox.showerror("ERROR!", "El proceso ya se ha corrido para alguno de los elementos seleccionados, volver a intentar con otra selección")
       
       #PROCESO PARA PRODUCTOS SIN RECETA
        elif(items_df['receta'] == 'SIN_RECETA').any():
            messagebox.showwarning("AVISO!", "Se ejecutará el proceso unicamente para aquellos productos SIN RECETA")
            # FILTRAR ITEMS DF PARA SIN RECETA
            # Keep only rows where 'receta' is 'SIN_RECeta'
            no_receta_df = items_df[items_df["receta"] == "SIN_RECETA"]
            no_receta_df_2 = no_receta_df.copy()
            no_receta_df = no_receta_df.rename(columns={"SKU": "CVE_ART", "CANT": "EXISTENCIA"})

            list_uni_entrada = []
            list_costo_entrada = []
            op = no_receta_df["OP"].iloc[0]
            vmax_nummov = variables_inventarios_entradas()
            vmax_nummov = vmax_nummov+1

            for i, row in no_receta_df.iterrows():
                cve_art = row["CVE_ART"]
                uni_costo_entrada = variables_entradas_inve(cve_art)
    
                unidad_entrada = uni_costo_entrada[0]
                costo_entrada = uni_costo_entrada[1]
                list_uni_entrada.append(unidad_entrada)
                list_costo_entrada.append(costo_entrada)


            no_receta_df["unidad_entrada"] = list_uni_entrada
            no_receta_df["costo_entrada"] = list_costo_entrada
            no_receta_df = no_receta_df.reset_index(drop=True)


            no_receta_df['ALMACEN'] = 1
    
    
            no_receta_df['FECHA_DOCU'] = datetime.now().strftime("%Y-%m-%d")
            no_receta_df['TIPO_DOC'] = "M"
            no_receta_df['REFER'] = "OP00000"+ str(op)
            
            no_receta_df['CVE_CPTO'] = 3
            no_receta_df['REG_SERIE'] = 0
            no_receta_df['TIPO_PROD'] = "P"
            no_receta_df['FACTOR_CON'] = 1
            no_receta_df['FECHAELAB'] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
            no_receta_df['CVE_FOLIO'] = 1
            no_receta_df['SIGNO'] = 1
            no_receta_df['COSTEADO'] = "S"
            no_receta_df['DESDE_INVE'] = "S"
            no_receta_df['MOV_ENLAZADO'] = 0
            no_receta_df['UNI_VENTA'] = "pz"
            no_receta_df['COSTO'] = no_receta_df['costo_entrada']
            no_receta_df['COSTO_PROM_INI'] = no_receta_df['costo_entrada']
            no_receta_df['COSTO_PROM_FIN'] = no_receta_df['costo_entrada']
            no_receta_df['COSTO_PROM_GRAL'] = no_receta_df['costo_entrada']

            no_receta_df['NUM_MOV'] = range(vmax_nummov, vmax_nummov + len(no_receta_df))

            # Reset index 
            no_receta_df = no_receta_df.reset_index(drop=True)

            no_receta_df['CANT'] = no_receta_df["EXISTENCIA"]
            no_receta_df['EXIST_G'] = no_receta_df["EXISTENCIA"]


            columnas_seleccionadas = ["CVE_ART", "ALMACEN", "NUM_MOV", "CVE_CPTO","FECHA_DOCU", "TIPO_DOC", "REFER", "CANT",
                                      "COSTO", "REG_SERIE", "UNI_VENTA", "EXIST_G",
                                      "EXISTENCIA", "TIPO_PROD", "FACTOR_CON", "FECHAELAB", 
                                      "CVE_FOLIO", "SIGNO", "COSTEADO", "COSTO_PROM_INI", 
                                      "COSTO_PROM_FIN", "COSTO_PROM_GRAL", "DESDE_INVE", "MOV_ENLAZADO"]
            entrada_no_receta = no_receta_df[columnas_seleccionadas]

            for thing, row in entrada_no_receta.iterrows():
                df = pd.DataFrame([row])
                #modifica las existencias en el almacen 1
                values_almacen_04 = almacen_04(df)

                exist_inve = values_almacen_04[0]
                current_exist = values_almacen_04[1]

                #inserta entradas en minve 04 - producto terminado
                insert_entradas(df, exist_inve, current_exist)

                #modifica las existencias del producto terminado
                insert_exist_entradas(df, exist_inve)
            messagebox.showinfo("Éxito", "Se agregaron entradas de producto terminado SIN RECETA con éxito")

            for j, row in no_receta_df_2.iterrows():
                op = row['OP']
                sku = row['SKU']
                cant = row['CANT']
                desv = row['DES']
                lote = row['LOTE']
                consumo = row['CONSUMO']
                folio = row['FOLIO']
                st_prod = row['ST. PRODUCCION']
                st_prod_nuevo = "Producido"
                producido(op, sku, desv, lote, consumo, folio, st_prod, st_prod_nuevo)

                self.tabla.delete(*self.tabla.get_children())
                self.lista_ctrl_op_tabla = query_ctrl_orden_produccion(op)
                self.lista_ctrl_op_tabla.reverse()
        
            for p in self.lista_ctrl_op_tabla:
                my_tag = "pass" if p[7] != "Por_producir" else "Por_producir"
                self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))

        #PROCESO COMPLETO PARA PRODUCTOS CON RECETA        
        else:
            for index, row in items_df.iterrows():
                op = row['OP']
                sku = row['SKU']
                cant = row['CANT']
                desv = row['DES']
                lote = row['LOTE']
                consumo = row['CONSUMO']
                folio = row['FOLIO']
                st_prod = row['ST. PRODUCCION']
                st_prod_nuevo = "Producido"

                data_materia_prima = materia_prima(sku, cant)
                materia_df = pd.concat([materia_df, data_materia_prima], ignore_index = False)
        
            list_uni = []   
            list_costo = []
            list_uni_entrada = []
            list_costo_entrada = []

            for i, row in materia_df.iterrows():
                cve_prod = row["CVE_PROD"]
                uni_costo = variables_salidad_inve(cve_prod)
                unidad = uni_costo[0]
                costo = uni_costo[1]
                list_uni.append(unidad)
                list_costo.append(costo)
                cve_art = row["CVE_ART"]
                uni_costo_entrada = variables_entradas_inve(cve_art)
                unidad_entrada = uni_costo_entrada[0]
                costo_entrada = uni_costo_entrada[1]
                list_uni_entrada.append(unidad_entrada)
                list_costo_entrada.append(costo_entrada)
            
        
            materia_df["unidad"] = list_uni
            materia_df["costo"] = list_costo
            materia_df["unidad_entrada"] = list_uni_entrada
            materia_df["costo_entrada"] = list_costo_entrada
            materia_df = materia_df.reset_index(drop=True)
            
            lote = items_df["LOTE"].iloc[0]

            salidas_df = salidas(materia_df, op)
            entradas_df = entradas(materia_df, op, items_df)

            for materias, row in salidas_df.iterrows():
                df = pd.DataFrame([row])

                #inserta salidas en minve 05
                insert_salidas(df)
                
                #modifica las existencias del producto terminado
                insert_exist_salidas(df)
                v=1
            messagebox.showinfo("Éxito", "Se agregaron salidas de materia prima con éxito")
            for thing, row in entradas_df.iterrows():
                df = pd.DataFrame([row])
                
                #modifica las existencias en el almacen 1
                values_almacen_04 = almacen_04(df)

                exist_inve = values_almacen_04[0]
                current_exist = values_almacen_04[1]

                #inserta entradas en minve 04 - producto terminado
                insert_entradas(df, exist_inve, current_exist)

                #modifica las existencias del producto terminado
                insert_exist_entradas(df, exist_inve)
            messagebox.showinfo("Éxito", "Se agregaron entradas de producto terminado con éxito")

            cat_pedimentos = select_lista_pedimentos()

            materia_df['condicion'] = materia_df['CVE_PROD'].isin(cat_pedimentos['materias']).map({True: 'si', False: 'no'})
            df_filtro = materia_df[materia_df["condicion"].str.contains('si', case=False)].reset_index(drop=True)



            #obtener las cantidades del PT y el lote, fecha de caducidad del df items
            df_filtro = df_filtro.merge(items_df, left_on='CVE_ART', right_on='SKU', how='left').reset_index(drop=True)

            #cambiarle el nombre a las columnas para manejar mejor
            df_filtro.drop(columns=['SKU'], inplace=True)
            df_filtro.rename(columns={'CANT': 'CANT_PT'}, inplace=True)
            # Aplicar la función a la columna "CONSUMO"
            df_filtro["CONSUMO"] = df_filtro["CONSUMO"].apply(convertir_fecha)

            #Eliminar los productos que tengan mas de una materia prima en su receta
            total_mp = df_filtro["CVE_ART"].value_counts()
            df_filtro = df_filtro[~df_filtro['CVE_ART'].isin(total_mp[total_mp > 1].index)].reset_index()

            #inicia 
            for index_p, row in df_filtro.iterrows():
                criterio = row['CVE_PROD']

                #obtener la lista de las mp que llevan pedimento
                lista = pedi_05(criterio)

                cant_mp = row['CantNec']
                lista['comparasion'] = ""

                # cantidad en negativo para restar
                lista_copy = lista.copy()

                # Iterate over each row in the DataFrame
                for i_p, row in lista_copy.iterrows():
                    #si la cantidad que hay de pedimento es mayor que la cantidad solicitada asignale la categoria de mayor 
                    #y resta la cantidad solicitada a la cantidad del pedimento
                    #sal del bucle
                    if row['CANTIDAD'] >= cant_mp:
                        lista_copy.at[i_p, 'comparasion'] = 'mayor'
                        lista_copy.at[i_p, "CANTIDAD"] -= cant_mp
                        break
                    else:
                    #si la cantidad del pedimento es menor a la cantidad solicitada, asignale la categoria de menor
                    # y resta la cantidad solicitada de la cantidad del pedimento
                    # continua el bucle hasta encontrar un pedimento  que sea mayor a la cantidad solicitada
                        lista_copy.at[i_p, 'comparasion'] = 'menor'
                        lista_copy.at[i_p, "CANTIDAD"] -= cant_mp
                #si se encontro pedimento para esa materia prima seleccionalo y sigue con el proceso
                if "SIN PEDIMENTO" not in lista_copy['PEDIMENTO'].values:
                    # Filter out rows where 'comparasion' column is empty
                    filtered_lista_copy = lista_copy[lista_copy['comparasion'] != ""]

                # Ajustar los valores de cantidad para que no queden valores negativos
                    for j_p, row in filtered_lista_copy.iterrows():
                        if filtered_lista_copy.at[j_p, "CANTIDAD"] < 0:
                            dif = filtered_lista_copy.at[j_p, "CANTIDAD"] + cant_mp
                            if j_p + 1 < len(filtered_lista_copy):
                                filtered_lista_copy.at[j_p + 1, "CANTIDAD"] += dif
                            filtered_lista_copy.at[j_p, "CANTIDAD"] = 0
                        else:
                            dif = 0
                    #asigna la fecha de ultimo movimiento
                    filtered_lista_copy["FCHULTMOV"] = datetime.now().strftime("%Y-%m-%d %H:%M:%S")

                    #extrae unicamente las columnas que son necesarias para actualizar los pedimentos
                    data_ltpd05 = filtered_lista_copy[["FCHULTMOV", "CANTIDAD", "REG_LTPD"]]

                    #Actualiza los pedimentos de las materias primas
                    update_ltpd05(data_ltpd05) #########ACTIVAR

                # Obtener el valor del pedimento mas nuevo de cada mp para los productos terminados
                    max_index = filtered_lista_copy["CANTIDAD"].idxmax()
                    pedimentos = filtered_lista_copy.loc[[max_index]]

                    #calcula el valor máximo de reg_ltpd04 para insertar nueva fila en pedimentos de PT
                    max_04 = calc_max_ltpd04()
                    pedimentos["REG_LTPD"] = max_04 + 1
                    

                    # Obtener los nombres de los productos terminados para ponerle los pedimentos
                    merged_df = pedimentos.merge(df_filtro, left_on='CVE_ART', right_on='CVE_PROD', how='left')
                    merged_df = merged_df.reset_index(drop = True)

                    merged_df.rename(columns={'CVE_ART_y': 'producto'}, inplace=True)
                    merged_df["CVE_ALM"] = 1
                    columns_to_keep = ["producto", "LOTE_y", "PEDIMENTO", "CVE_ALM", "CONSUMO", "FCHADUANA",
                                        "FCHULTMOV", 'NOM_ADUAN', 'CANT_PT', 'REG_LTPD',
                                        'CVE_OBS', 'CIUDAD', "FRONTERA", "FEC_PROD_LT", 'GLN',
                                        'STATUS', 'PEDIMENTOSAT']
                    #seleccionar las columnas que queremos que se queden para insertarlas en ltpd04
                    merged_df = merged_df[columns_to_keep]
                    #asignar la fecha de produccion del PT
                    merged_df["FEC_PROD_LT"] = datetime.now().strftime("%Y-%m-%d")

                    #convierte ese df a lista
                    insercion_list = merged_df.values.tolist()
                    

                    #inserta los valores en ltpd 04
                    insert_ltpd04(insercion_list) ###########ACTIVAR


                else: #si no se encontro pedimento de materias primas, buscar si esta en los pedimentos de PT esa materia prima
                        # Buscar el valor del pedimento
                        materias_04 = buscar_pedi_04(criterio)
                                    # Máximo reg_ltpd04
                        max_04 = calc_max_ltpd04()
                        materias_04["REG_LTPD"] = max_04 + 1
                        

                        # Asignarle el nombre de esa mp a PT
                        merged_df_sp = materias_04.merge(df_filtro, left_on='CVE_ART', right_on='CVE_PROD', how='left')
                        merged_df_sp = merged_df_sp.reset_index(drop = True)
                        merged_df_sp.rename(columns={'CVE_ART_y': 'producto'}, inplace=True)
                        merged_df_sp["CVE_ALM"] = 1
                        columns_to_keep_sp = ["producto", "LOTE_y", "PEDIMENTO", "CVE_ALM", "CONSUMO", "FCHADUANA",
                                        "FCHULTMOV", 'NOM_ADUAN', 'CANT_PT', 'REG_LTPD',
                                        'CVE_OBS', 'CIUDAD', "FRONTERA", "FEC_PROD_LT", 'GLN',
                                        'STATUS', 'PEDIMENTOSAT']
                        merged_df_sp = merged_df_sp[columns_to_keep_sp]
                        merged_df_sp["FEC_PROD_LT"] = datetime.now().strftime("%Y-%m-%d")
                        merged_df_sp["STATUS"] = "A"
                        
                                     
                    #si no hay pedimento se para el proceso, solo continua para los df donde se encontro pedimento de mp en PT
                        if "SIN PEDIMENTO" not in merged_df_sp['PEDIMENTO'].values:
                            insercion_list_04 = merged_df_sp.values.tolist()
                            insert_ltpd04(insercion_list_04) ##########ACTIVAR
                        else:
                            merged_df_sp['PEDIMENTO'] = ''
                            merged_df_sp['FCHULTMOV'] = datetime.now()
                            merged_df_sp['CVE_OBS'] = 0

                            merged_df_sp = merged_df_sp.fillna('')
                            insercion_list_04 = merged_df_sp.values.tolist()
                            insert_ltpd04(insercion_list_04) ##########ACTIVAR                     

  
            update_ctrl_05()
            update_ctrl_04()           
            messagebox.showinfo("Éxito", "Se agregaron pedimentos de producto terminado")
 
            for j, row in items_df.iterrows():
                op = row['OP']
                sku = row['SKU']
                cant = row['CANT']
                desv = row['DES']
                lote = row['LOTE']
                consumo = row['CONSUMO']
                folio = row['FOLIO']
                st_prod = row['ST. PRODUCCION']
                st_prod_nuevo = "Producido"
                producido(op, sku, desv, lote, consumo, folio, st_prod, st_prod_nuevo)

                self.tabla.delete(*self.tabla.get_children())
                self.lista_ctrl_op_tabla = query_ctrl_orden_produccion(op)
                self.lista_ctrl_op_tabla.reverse()
        
            for p in self.lista_ctrl_op_tabla:
                my_tag = "pass" if p[7] != "Por_producir" else "Por_producir"
                self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))

    def open_main_window(self):
        hide_all_frames(self.master)
        niv_user = self.niv_user
        opciones_window = opcioneswindow(self.master, niv_user)
        opciones_window.pack(fill="both", expand=True)

    def open_modificar_consumo(self):
        selected_item = self.tabla.selection()
        items_df = []
        for item in selected_item:
            current_item = self.tabla.item(item)['values']
            items_df.append(current_item)
        items_df = pd.DataFrame(items_df, columns=["OP", "SKU", "CANT", "DES", "LOTE", "CONSUMO", "FOLIO", "ST. PRODUCCION", "receta", "tipo"])
        length = len(items_df)
        if length == 1:
            op = items_df.iloc[0]['OP']
            sku = items_df.iloc[0]['SKU']
            cant = items_df.iloc[0]['CANT']
            desv = items_df.iloc[0]['DES']
            lote = items_df.iloc[0]['LOTE']
            consumo = items_df.iloc[0]['CONSUMO']
            folio = items_df.iloc[0]['FOLIO']
            st_prod = items_df.iloc[0]['ST. PRODUCCION']

            if st_prod == "Producido":
                messagebox.showerror("Error", "Elemento producido, no se puede realizar modificaciones")
            else:
                modificar_open = modificar_consumo(self, self, op, sku, cant, desv, lote, consumo, folio, st_prod)
        else:
            messagebox.showerror("Error", "Selecciona solo 1 registro para realizar la modificación")

    def open_modificar_lote(self):
        selected_item = self.tabla.selection()
        items_df = []
        for item in selected_item:
            current_item = self.tabla.item(item)['values']
            items_df.append(current_item)
        items_df = pd.DataFrame(items_df, columns=["OP", "SKU", "CANT", "DES", "LOTE", "CONSUMO", "FOLIO", "ST. PRODUCCION", "receta", "tipo"])
        length = len(items_df)
        if length == 1:
            op = items_df.iloc[0]['OP']
            sku = items_df.iloc[0]['SKU']
            cant = items_df.iloc[0]['CANT']
            desv = items_df.iloc[0]['DES']
            lote = items_df.iloc[0]['LOTE']
            consumo = items_df.iloc[0]['CONSUMO']
            folio = items_df.iloc[0]['FOLIO']
            st_prod = items_df.iloc[0]['ST. PRODUCCION']

            if st_prod == "Producido":
                messagebox.showerror("Error", "Elemento producido, no se puede realizar modificaciones")
            else:
                modificar_open = modificar_lote(self, self, op, sku, cant, desv, lote, consumo, folio, st_prod)
        else:
            messagebox.showerror("Error", "Selecciona solo 1 registro para realizar la modificación")

    def refresh_table(self):
        self.folio_ctrl_op = self.my_folio.get()
        self.lista_ctrl_op_tabla = query_ctrl_orden_produccion(self.folio_ctrl_op)
        self.lista_ctrl_op_tabla.reverse()
        my_tag = "normal" #default tag

        self.tabla.delete(*self.tabla.get_children())
        for p in self.lista_ctrl_op_tabla:
            my_tag = "pass" if p[7] != "Por_producir" else "Por_producir"
            self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))

class modificar_consumo(tk.Toplevel):
    def __init__(self, parent, master, op, sku, cant, desv, lote, consumo, folio, st_prod):
        super().__init__(master)
        self.geometry("477x300")
        self.resizable(0,0)
        self.columnconfigure((0, 1, 2), weight=1)
        self.rowconfigure((0, 1, 2, 3), weight=1)

        self.title("Modificar Fecha de Consumo")

        self.op = op
        self.sku = sku
        self.cant = cant
        self.desv = desv
        self.lote = lote
        self.consumo = consumo
        self.folio = folio
        self.st_prod = st_prod

        self.campos = tk.Frame(self)
        self.campos.grid(row = 1, column = 0, columnspan=3)

        title_label = tk.Label(self, text="Modificar Fecha de Consumo")
        title_label.config(font=("Roboto", 20, "bold"), anchor="center")
        title_label.grid(row=0, column=1, padx=10, pady=10)
        self.parent = parent

        self.grab_set()

        
        self.campos_entry()
    def campos_entry(self):
    # User label
        antigua_label = tk.Label(self.campos, text="Antigua Fecha de Consumo")
        antigua_label.config(font=("Roboto", 16, "bold"), anchor="center")
        antigua_label.grid(row=0, column=0)

    # # User entry
        antigua_entry = tk.Entry(self.campos, state = "normal")
        antigua_entry.config(width=12, font=("Roboto", 16))
        antigua_entry.grid(row=0, column=1, sticky="nsew", padx = 10, pady = 10)
        antigua_entry.insert(0, self.consumo)
        antigua_entry.config(state = "disabled")

    #nueva label
        nueva_label = tk.Label(self.campos, text="Nueva Fecha de Consumo")
        nueva_label.config(font=("Roboto", 16, "bold"), anchor="center")
        nueva_label.grid(row=1, column=0)

    #nueva entry
        self.my_nueva = tk.StringVar()
        nueva_entry = tk.Entry(self.campos, textvariable=self.my_nueva)
        nueva_entry.config(width=12, font=("Roboto", 16))
        nueva_entry.grid(row=1, column=1, sticky="nsew",  padx = 10, pady = 10)

                # Guardar usuario button
        guardar_button = ctk.CTkButton(self.campos, text="Guardar\nCambios", command = self.guardar_consumo_nuevo)
        guardar_button.configure(width=100, height=50, font=("Roboto", 16, "bold"), anchor="center", border_color = "black",
                               border_width = 2)
        guardar_button.grid(row=4, column=0, padx=10, pady=10, columnspan = 2)

    def guardar_consumo_nuevo(self):
        print("SI")
        self.consumo_nuevo = self.my_nueva.get()
        self.op = int(self.op)
        self.cant = int(self.cant)
        self.sku = str(self.sku)
        # self.lote = int(self.lote)
        self.consumo_nuevo = str(self.consumo_nuevo)
        fecha_obj = datetime.strptime(self.consumo_nuevo, "%m/%Y")
        fecha_hoy = datetime.now()

        dos_anos = timedelta(days=2*365)
        try:
            fecha_obj = datetime.strptime(self.consumo_nuevo, "%m/%Y")
            fecha_hoy = datetime.now()

            dos_anos = timedelta(days=2*365)

            if fecha_obj - fecha_hoy > dos_anos:
                messagebox.showerror("ERROR!", "Fecha de consumo debe ser menor a 2 años")
            else:
                try:
                    consumo_nuevo(self.op, self.sku, self.consumo_nuevo)
                    title = "Exito"
                    message = "Se modificó la fecha de consumo"
                    messagebox.showinfo(title, message)
                    self.parent.refresh_table()
                    self.destroy()

                except Exception as e:
                    titulo = "Error en funcion"
                    mensaje = f"Error: {str(e)}"
                    messagebox.showerror(titulo, mensaje)
                    print(e)
        except ValueError:
            # Capturar error de formato de fecha
            messagebox.showerror("ERROR!", "Formato de fecha inválido. Use MM/AAAA.")

class modificar_lote(tk.Toplevel):
    def __init__(self, parent, master, op, sku, cant, desv, lote, consumo, folio, st_prod):
        super().__init__(master)
        self.geometry("477x300")
        self.resizable(0,0)
        self.columnconfigure((0, 1, 2), weight=1)
        self.rowconfigure((0, 1, 2, 3), weight=1)

        self.title("Modificar Lote")

        self.op = op
        self.sku = sku
        self.cant = cant
        self.desv = desv
        self.lote = lote
        self.consumo = consumo
        self.folio = folio
        self.st_prod = st_prod

        self.campos = tk.Frame(self)
        self.campos.grid(row = 1, column = 0, columnspan=3)

        title_label = tk.Label(self, text="Modificar Lote")
        title_label.config(font=("Roboto", 20, "bold"), anchor="center")
        title_label.grid(row=0, column=1, padx=10, pady=10)
        self.parent = parent
        self.grab_set()


        self.campos_entry()

    def campos_entry(self):
    # User label
        antigua_label = tk.Label(self.campos, text="Antiguo Lote")
        antigua_label.config(font=("Roboto", 16, "bold"), anchor="center")
        antigua_label.grid(row=0, column=0)

    # # User entry
        antigua_entry = tk.Entry(self.campos, state = "normal")
        antigua_entry.config(width=12, font=("Roboto", 16))
        antigua_entry.grid(row=0, column=1, sticky="nsew", padx = 10, pady = 10)
        antigua_entry.insert(0, self.lote)
        antigua_entry.config(state = "disabled")

    #nueva label
        nueva_label = tk.Label(self.campos, text="Nuevo Lote")
        nueva_label.config(font=("Roboto", 16, "bold"), anchor="center")
        nueva_label.grid(row=1, column=0)

    #nueva entry
        self.my_nueva = tk.StringVar()
        nueva_entry = tk.Entry(self.campos, textvariable=self.my_nueva)
        nueva_entry.config(width=12, font=("Roboto", 16))
        nueva_entry.grid(row=1, column=1, sticky="nsew",  padx = 10, pady = 10)

                # Guardar usuario button
        guardar_button = ctk.CTkButton(self.campos, text="Guardar\nCambios", command=self.guardar_lote_nuevo)
        guardar_button.configure(width=100, height=50, font=("Roboto", 16, "bold"), anchor="center", border_color = "black",
                               border_width = 2)
        guardar_button.grid(row=4, column=0, padx=10, pady=10, columnspan = 2)

    def guardar_lote_nuevo(self):
        self.lote_nuevo = self.my_nueva.get()
        self.op = int(self.op)
        self.cant = int(self.cant)
        self.sku = str(self.sku)
        self.lote = str(self.lote)
        self.lote_nuevo = str(self.lote_nuevo)
        # self.folio = int(self.folio)
        
        try:
            modificar_lote_nuevo(self.op, self.sku, self.lote_nuevo)
            title = "Exito"
            message = "Se modificó el lote del producto"
            messagebox.showinfo(title, message)
            self.parent.refresh_table()
            self.destroy()

        except Exception as e:
            titulo = "Error en funcion"
            mensaje = f"Error: {str(e)}"
            messagebox.showerror(titulo, mensaje)
            print(e)

class utilerias(tk.Frame):
    def __init__(self, master, niv_user):
        super().__init__(master)

        self.user_niv = niv_user

        self.columnconfigure((0,1,2), weight = 1)
        self.rowconfigure((0,1,2,3), weight = 1)

        login_label= tk.Label(self, text = "Comercializadora Sueca de México, S.A. DE C.V.")
        login_label.config(font=("Roboto", 22, "bold"), anchor="center")
        login_label.grid(row = 0, column = 0, columnspan=3)

        #dia de trabajo
        self.date_label = tk.Label(self, text = "")
        self.date_label.grid(row = 1, column= 0, columnspan=3)
        self.date_label.config(font=("Roboto", 22, "bold"), anchor="center")

        #frame for all the options
        self.opciones_frame = tk.Frame(self)
        self.opciones_frame.grid(row = 2, column = 1, padx=10, pady =10)

        self.salir_frame = tk.Frame(self)
        self.salir_frame.grid(row = 3, column = 1, padx=10, pady =10)

        self.opciones()
        self.update_date()
 
    def update_date(self):
     # Get the current date and format it
        today_date = datetime.now().strftime("%Y-%m-%d")

    # Update the label text
        self.date_label.config(text="Día de trabajo: " + today_date)

    def opciones(self):
        #options
        #img admin users
        img_delete_users_path = resource_path("img/users.png")
        img_delete_users = Image.open(img_delete_users_path)
        resized_img_delete_users= img_delete_users.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_image = ImageTk.PhotoImage(resized_img_delete_users)

        #buton admin users
        self.button_delete_user = ctk.CTkButton(self.opciones_frame, image = self.converted_image, text = "Admin. usuarios", 
                                                command=self.open_eliminar_users)
        self.button_delete_user.configure(width = 300, height = 50,
                                            fg_color ="#3B8ED0", text_color="black", hover_color="#36719F",
                                            border_width = 2,
                                            font=("Roboto", 16, "bold"),
                                            compound = tk.LEFT)
        self.button_delete_user.grid(row = 0, column = 0, pady = 10)

            #boton home 
    #img home
        img_home_path = resource_path("img/home.png")
        img_home = Image.open(img_home_path)
        resized_img_home = img_home.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_home = ImageTk.PhotoImage(resized_img_home)

        #buton salir a home
        self.boton_salir = ctk.CTkButton(self.opciones_frame, image = self.converted_img_home, text = "Home", command = self.open_main_window)
        self.boton_salir.configure(width = 200, height = 30,
                                   fg_color ="#FF5050", text_color="black", hover_color="#DD1818",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_salir.grid(row = 1, column = 0, pady = 10)

    def open_eliminar_users(self):
         eliminar_open = eliminar_usuarios(self.master)


    def open_main_window(self):
        hide_all_frames(self.master)
        niv_user = self.user_niv
        print(niv_user)
        opciones_window = opcioneswindow(self.master, niv_user)
        opciones_window.pack(fill="both", expand=True)

class eliminar_usuarios(tk.Toplevel):
    def __init__(self, master):
        super().__init__(master)
        # self.geometry("477x300")
        # self.resizable(0,0)
        self.columnconfigure((0, 1, 2), weight=1)
        self.rowconfigure((0, 1, 2, 3), weight=1)

        self.campos_frame = tk.Frame(self)
        self.campos_frame.grid(row = 0, column = 0, padx = 10, pady=10, sticky="w")


        self.tabla_frame = tk.Frame(self)
        self.tabla_frame.grid(row = 1, column = 0, padx = 10, pady = 10, sticky="w")

        self.campos_botones = tk.Frame(self)
        self.campos_botones.grid(row = 2, column = 0, padx = 10, pady = 10, sticky="w")

        self.title("Eliminar usuario")
        self.grab_set()

        self.tabla_users()
        self.campos()
        self.deshabilitar_campos()
    def campos(self):

        # label
        self.nombre_label= tk.Label(self.campos_frame, text = "Nombre:")
        self.nombre_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.nombre_label.grid(row = 0, column = 0, sticky = "w", padx=10, pady=10)

        #entry
        self.my_nombre = tk.StringVar()
        self.entry_nombre = tk.Entry(self.campos_frame, textvariable = self.my_nombre)
        self.entry_nombre.configure(width = 15 , font = ("Roboto", 12))
        self.entry_nombre.grid(row = 0, column = 1, sticky = "w", padx=10, pady=10)

                # label
        self.nivel_label = tk.Label(self.campos_frame, text = "Nivel:")
        self.nivel_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.nivel_label.grid(row = 1, column = 0, sticky = "w", padx=10, pady=10)

        #entry
        self.my_nivel = tk.StringVar()
        self.entry_nivel = tk.Entry(self.campos_frame, textvariable = self.my_nivel)
        self.entry_nivel.configure(width = 15 , font = ("Roboto", 12))
        self.entry_nivel.grid(row = 1, column = 1, sticky = "w", padx=10, pady=10)

                        # label
        self.pass_label = tk.Label(self.campos_frame, text = "Nueva Clave:")
        self.pass_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.pass_label.grid(row = 2, column = 0, sticky = "w", padx=10, pady=10)

        #entry
        self.my_pass = tk.StringVar()
        self.entry_pass = tk.Entry(self.campos_frame, textvariable = self.my_pass, show = "*")
        self.entry_pass.configure(width = 15 , font = ("Roboto", 12))
        self.entry_pass.grid(row = 2, column = 1, sticky = "w", padx=10, pady=10)

                #boton edit
        img_edit_path = resource_path("img/edit.png")
        img_edit = Image.open(img_edit_path)
        resized_img_edit = img_edit.resize((20,20), Image.Resampling.LANCZOS)
        self.converted_img_edit = ImageTk.PhotoImage(resized_img_edit)

        #buton edit  
        self.boton_edit = ctk.CTkButton(self.campos_botones, text = "Editar", image = self.converted_img_edit, command=self.editar)
        self.boton_edit.configure(width = 200, height = 25,
                                  fg_color ="#90F37E", text_color="black", hover_color="#26A810",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_edit.grid(row = 0, column = 0, padx = 5, sticky = "w")

                        #boton eliminar
        img_eliminar_path = resource_path("img/trash.png")
        img_eliminar = Image.open(img_eliminar_path)
        resized_img_eliminar = img_eliminar.resize((20,20), Image.Resampling.LANCZOS)
        self.converted_img_eliminar = ImageTk.PhotoImage(resized_img_eliminar)

        #buton eliminar  
        self.boton_eliminar = ctk.CTkButton(self.campos_botones, text = "Eliminar", image = self.converted_img_eliminar, command=self.eliminar)
        self.boton_eliminar.configure(width = 200, height = 25,
                                  fg_color ="#FF3535", text_color="black", hover_color="#B80000",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_eliminar.grid(row = 0, column = 1, padx = 5, sticky = "w")

                                #boton eliminar
        img_save_path = resource_path("img/archivo.png")
        img_save = Image.open(img_save_path)
        resized_img_save = img_save.resize((20,20), Image.Resampling.LANCZOS)
        self.converted_img_save = ImageTk.PhotoImage(resized_img_save)

        #buton save 
        self.boton_save = ctk.CTkButton(self.campos_botones, text = "Guardar", image = self.converted_img_save, command = self.guardar)
        self.boton_save.configure(width = 200, height = 25,
                                  fg_color ="#A3BCF9", text_color="black", hover_color="#3F72F3",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_save.grid(row = 0, column = 2, padx = 5, sticky = "w")

    def tabla_users(self):
        self.lista_users = listar_users()

        #create table to show the entrys
        self.tabla = ttk.Treeview(self.tabla_frame,
                                  column = ("numero","nombre", "nivel"),
                                  height = 20,
                                  show =  "headings",
                                  selectmode="extended"
                                  )
        
        # Add headings
        self.tabla.heading("numero", text="Número")
        self.tabla.heading("nombre", text="Nombre")
        self.tabla.heading("nivel", text="Nivel")

        # Configure column width
        self.tabla.column("numero", width=200)
        self.tabla.column("nombre", width=200)
        self.tabla.column("nivel", width = 200)

        # Pack the treeview inside the productos tab frame
        self.tabla.grid(row = 0, column = 0,sticky="w")

      #iteration over lista e producciones programadas
        for p in self.lista_users:
            self.tabla.insert('', 0, text=p[0], values=(p[0:]))

    def deshabilitar_campos(self):
        #reset id 
        #clean entry fields

        self.my_nombre.set("")
        self.my_nivel.set("")
        self.my_pass.set("")
        
        #disable the entry fields
        # self.entry_num.config(state = "disabled")
        self.entry_nombre.config(state = "disabled")
        self.entry_nivel.config(state = "disabled")
        self.entry_pass.config(state = "disabled")
        
        #disable buttons
        self.boton_save.configure(state = "disabled")
    
    def habilitar_campos(self):
       #clean entry fields

        self.my_nombre.set("")
        self.my_nivel.set("")

        #enable the entry field

        self.entry_nombre.config(state = "normal")
        self.entry_nivel.config(state = "normal")
        self.entry_pass.config(state = "normal")

        #enable buttons
        self.boton_save.configure(state = "normal")

    def editar(self):
        try:
            #recuperar los datos de la tabla en los campos
            self.nivel_user = self.tabla.item(self.tabla.selection())['values'][2]
            self.nombre_user = self.tabla.item(self.tabla.selection())['values'][1]
            self.numero_user = self.tabla.item(self.tabla.selection())['values'][0]
            self.habilitar_campos()


            self.entry_nombre.insert(0,self.nombre_user)
            self.entry_nivel.insert(0,self.nivel_user)


        except Exception as e:
            titulo = "Error editar"
            mensaje = f"Error: {str(e)}"
            messagebox.showerror(titulo, mensaje)
            print(e)

    def guardar(self):
        
        lista_save = [self.my_nivel.get(), self.my_nombre.get(), self.my_pass.get()]
        lista_save = [item.strip() for item in lista_save]

        guardar(lista_save, self.numero_user)

        self.tabla_users()
        
        #disable fields
        self.deshabilitar_campos()

        messagebox.showinfo("Éxito!", "La información se actualizó con éxito")
        self.destroy()

    def eliminar(self):
        try:
            #recuperar los datos de la tabla en los campos
            self.numero_user = self.tabla.item(self.tabla.selection())['values'][0]
            self.nombre_user = self.tabla.item(self.tabla.selection())['values'][1]
            eliminar_user(self.numero_user)
            self.tabla_users()
            messagebox.showinfo("", f"Usuario {self.numero_user}, {self.nombre_user} eliminado")



        except Exception as e:
            titulo = "Error"
            mensaje = f"Error: {str(e)}"
            messagebox.showerror(titulo, mensaje)
            print(e)

class ctrl_orden_compra_window(tk.Frame):
    def __init__(self, master, niv_user):
        super().__init__(master)

        self.niv_user = niv_user

        self.columnconfigure((0,1,2,3,4,5), weight = 1)
        # self.rowconfigure((0,1,2,3), weight = 1)

        self.folio_frame = tk.Frame(self)
        self.folio_frame.grid(row = 0, column = 0, padx = 10, pady = 10, sticky = "nw")

        self.tabla_frame = tk.Frame(self)
        self.tabla_frame.grid(row =1, column = 0)
        self.botones_frame = tk.Frame(self)
        self.botones_frame.grid(row = 1, column = 2, padx = 10, pady = 10, sticky = "nw")

        self.campos()
        self.tabla_ctrl_produccion()

    def campos(self):
        #folio label
        self.folio_label = tk.Label(self.folio_frame, text = "Folio:")
        self.folio_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.folio_label.grid(row = 0, column = 0, sticky = "w")

        #entry folio
        self.my_folio = tk.StringVar()
        self.entry_folio = tk.Entry(self.folio_frame, textvariable = self.my_folio)
        self.entry_folio.configure(width = 15 , font = ("Roboto", 12))
        self.entry_folio.grid(row = 0, column = 1, sticky = "w")

        #boton buscar folio
        img_buscar_path = resource_path("img/lupa.png")
        img_buscar = Image.open(img_buscar_path)
        resized_img_buscar = img_buscar.resize((20,20), Image.Resampling.LANCZOS)
        self.converted_img_buscar = ImageTk.PhotoImage(resized_img_buscar)

        #buton buscar folio  
        self.boton_folio = ctk.CTkButton(self.folio_frame, text = "", image = self.converted_img_buscar, command=self.search_pendientes)
        self.boton_folio.configure(width = 25, height = 25,
                                   border_width = 2)
        self.boton_folio.grid(row = 0, column = 2, padx = 5, sticky = "w")

        #boton home 
    #img home
        img_home_path = resource_path("img/home.png")
        img_home = Image.open(img_home_path)
        resized_img_home = img_home.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_home = ImageTk.PhotoImage(resized_img_home)

        #buton salir a home
        self.boton_salir = ctk.CTkButton(self.botones_frame, image = self.converted_img_home, text = "Home", command = self.open_main_window)
        self.boton_salir.configure(width = 200, height = 30,
                                   fg_color ="#FF5050", text_color="black", hover_color="#DD1818",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_salir.grid(row = 4, column = 0, pady = 10)

        #boton surtir
        img_surtir_path = resource_path("img/requi.png")
        img_surtir = Image.open(img_surtir_path)
        resized_img_surtir = img_surtir.resize((60,60), Image.Resampling.LANCZOS)
        self.converted_img_surtir = ImageTk.PhotoImage(resized_img_surtir)

        self.boton_surtir = ctk.CTkButton(self.botones_frame, text = "Agregar\na Producción", image =self.converted_img_surtir, command=self.crear_requi)
        self.boton_surtir.configure(width = 200, height = 50,
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_surtir.grid(row = 0, column = 0, padx = 10, pady = 10)
    
    def tabla_ctrl_produccion(self):
     #create table to show the entrys
        self.tabla = ttk.Treeview(self.tabla_frame,
                                  column = ("folio","sku", "desvari", "cantidad", "N_S"),
                                  height = 30,
                                  show =  "headings",
                                  selectmode="extended"
                                  )
        
        # Add headings
        self.tabla.heading("folio", text="folio")
        self.tabla.heading("sku", text="SKU")
        self.tabla.heading("desvari", text="Descripción")
        self.tabla.heading("cantidad", text="Cant.")
        self.tabla.heading("N_S", text="Estado")

    # Configure column width
        self.tabla.column("folio", width=100)
        self.tabla.column("sku", width=100)
        self.tabla.column("desvari", width=300)
        self.tabla.column("cantidad", width = 75)
        self.tabla.column("N_S", width=200) 

        self.tabla.tag_configure("pass", background = "#ACF5A7") #surtido
        self.tabla.tag_configure("Falta_Materia_Prima", background = "#FF5247") #no surtido

    # Pack the treeview inside the productos tab frame
        self.tabla.grid(row = 0, column = 0, columnspan = 5, padx=5, pady=5, sticky = "ew")

        #add scrollbar
        self.scroll = ttk.Scrollbar(self.tabla_frame,
                                    orient = "vertical", 
                                    command = self.tabla.yview)
        self.scroll.grid(row = 0, column = 5, sticky = "nse")
        self.tabla.configure(yscrollcommand = self.scroll.set)


        self.folio_ctrl_compra = self.my_folio.get()
        self.lista_ctrl_compra_tabla = query_ctrl_pt_falta(self.folio_ctrl_compra)
        self.lista_ctrl_compra_tabla.reverse()
        # my_tag = "normal" #default tag

    def search_pendientes(self):

        self.folio_ctrl_compra = self.my_folio.get()
        if self.folio_ctrl_compra ==  "":
            self.tabla.delete(*self.tabla.get_children())
            self.lista_ctrl_compra_tabla = query_ctrl_pt_falta(self.folio_ctrl_compra)
            self.lista_ctrl_compra_tabla.reverse()

            for p in self.lista_ctrl_compra_tabla:
                my_tag = "pass" if p[4] != "Falta_Materia_Prima" else "Falta_Materia_Prima"
                self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))
        else:
            self.tabla.delete(*self.tabla.get_children())
            self.lista_ctrl_compra_tabla = query_ctrl_pt_falta(self.folio_ctrl_compra)
            self.lista_ctrl_compra_tabla.reverse()

            for p in self.lista_ctrl_compra_tabla:
                my_tag = "pass" if p[4] != "Falta_Materia_Prima" else "Falta_Materia_Prima"
                self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))

    def open_main_window(self):
        hide_all_frames(self.master)
        niv_user = self.niv_user
        opciones_window = opcioneswindow(self.master, niv_user)
        opciones_window.pack(fill="both", expand=True)

    def crear_requi(self):
        selected_items = self.tabla.selection()
        items_df = []
        for item in selected_items:
            current_item = self.tabla.item(item)['values']
            items_df.append(current_item)
        items_df = pd.DataFrame(items_df, columns=["Folio", "skuvari", "desvari", "cantidad", "estado"])
        items_df["skuvari"] = items_df["skuvari"].str.strip()
        if len(items_df) == 0:
            messagebox.showerror("ERROR!", "Favor de hacer alguna selección")
        else: 
            if (items_df['estado'] == 'Por_producir').any():
                messagebox.showerror("ERROR!", "Proceso ejecutado previamente, favor de hacer otra selección")
            else:
                self.items_df = items_df
                self.open_lote_consumo()
    
        self.tabla.delete(*self.tabla.get_children())
        self.lista_ctrl_compra_tabla = query_ctrl_pt_falta(self.folio_ctrl_compra)
        self.lista_ctrl_compra_tabla.reverse()
    
        for p in self.lista_ctrl_compra_tabla:
                my_tag = "pass" if p[4] != "Falta_Materia_Prima" else "Falta_Materia_Prima"
                self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))

    def open_lote_consumo(self):
        modificar_open = lote_consumo(self, self.items_df)

    def refresh_table(self):
        self.tabla.delete(*self.tabla.get_children())
        self.lista_ctrl_compra_tabla = query_ctrl_pt_falta(self.folio_ctrl_compra)
        self.lista_ctrl_compra_tabla.reverse()

        for p in self.lista_ctrl_compra_tabla:
            my_tag = "pass" if p[4] != "Falta_Materia_Prima" else "Falta_Materia_Prima"
            self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))
        
class lote_consumo(tk.Toplevel):
    def __init__(self, parent, items_df):
        super().__init__(parent)
        self.items_df = items_df  # Store the DataFrame
        self.geometry("477x300")
        self.resizable(0,0)
        self.columnconfigure((0, 1, 2), weight=1)
        self.rowconfigure((0, 1, 2, 3), weight=1)

        self.parent = parent

        self.title("Asignar Lote y Fecha de Consumo")

        self.campos = tk.Frame(self)
        self.campos.grid(row = 1, column = 0, columnspan=3)

        title_label = tk.Label(self, text="Asignar Lote y Fecha de Consumo")
        title_label.config(font=("Roboto", 20, "bold"), anchor="center")
        title_label.grid(row=0, column=1, padx=10, pady=10)
        self.grab_set()


        self.campos_entry()

    def campos_entry(self):
    # lote label
        lote_label = tk.Label(self.campos, text="Lote")
        lote_label.config(font=("Roboto", 16, "bold"), anchor="center")
        lote_label.grid(row=0, column=0)

    # lote entry
        self.my_lote = tk.StringVar()
        lote_entry = tk.Entry(self.campos, textvariable=self.my_lote)
        lote_entry.config(width=12, font=("Roboto", 16))
        lote_entry.grid(row=0, column=1, sticky="nsew", padx = 10, pady = 10)

    #consumo label
        consumo_label = tk.Label(self.campos, text="Consumo")
        consumo_label.config(font=("Roboto", 16, "bold"), anchor="center")
        consumo_label.grid(row=1, column=0)

    #consumo entry
        self.my_consumo = tk.StringVar()
        consumo_entry = tk.Entry(self.campos, textvariable=self.my_consumo)
        consumo_entry.config(width=12, font=("Roboto", 16))
        consumo_entry.grid(row=1, column=1, sticky="nsew",  padx = 10, pady = 10)

    # Guardar 
        guardar_button = ctk.CTkButton(self.campos, text="Agregar", command=self.guardar_variables)
        guardar_button.configure(width=100, height=50, font=("Roboto", 16, "bold"), anchor="center", border_color = "black",
                               border_width = 2)
        guardar_button.grid(row=4, column=0, padx=10, pady=10, columnspan = 2)

    def guardar_variables(self):
        self.lote = self.my_lote.get()
        self.consumo = self.my_consumo.get()

        fecha_obj = datetime.strptime(self.consumo, "%m/%Y")
        fecha_hoy = datetime.now()

        dos_anos = timedelta(days=2*365)
        try:
            fecha_obj = datetime.strptime(self.consumo, "%m/%Y")
            fecha_hoy = datetime.now()

            dos_anos = timedelta(days=2*365)

            if fecha_obj - fecha_hoy > dos_anos:
                messagebox.showerror("ERROR!", "Fecha de consumo debe ser menor a 2 años")
            else:
                try:
                    v_max = max_op()
                    self.items_df["OP"] = v_max
                    self.items_df["lote"] = self.lote
                    self.items_df["consumo"] = self.consumo
                    self.items_df["tipo"] = "Compra"
                    self.items_df["estado"] = "Por_producir"
          
                    #run the function recetas to know all the sku that have a recipe
                    receta = recetas()
                #create an empty list            
                    receta_df = []

                #iterate over each row of the df in the column skuvari
                    for index, row in self.items_df.iterrows():
                        if row['skuvari'] in receta:
                            receta_df.append('CON_RECETA')
                        else:
                            receta_df.append('SIN_RECETA')
                #add the results of the previous loop to the df
                    self.items_df["Receta"] = receta_df
                    # Convert the 'cantidad' column to numeric, forcing errors to NaN
                    self.items_df['cantidad'] = pd.to_numeric(self.items_df['cantidad'], errors='coerce')

                    # Fill NaN values with a default value or drop them
                    self.items_df['cantidad'] = self.items_df['cantidad'].fillna(0).astype(int)

                    insert_compras(self.items_df)

                    for i, row in self.items_df.iterrows():
                        skuvari = row["skuvari"]
                        folio = row["Folio"]
                        n_s = 'Por_producir'
                        update_estado_compras(n_s, skuvari, folio)
                    self.items_df["OP"]
                    compra_df = pd.DataFrame({
                                'OP': self.items_df["OP"],
                                'Cantidad': self.items_df["cantidad"],
                                'SKU': self.items_df["skuvari"],
                                'Descripcion': self.items_df["desvari"],
                                'Lote': self.items_df["lote"],
                                'Consumo':self.items_df["consumo"],
                                'Folio': self.items_df["Folio"]
                            })
                    
                    title = "Exito"
                    message = f"Productos agregados a OP #{v_max}"
                    messagebox.showinfo(title, message)

                    file_path = resource_path(f"client/OP_PT-Faltantes_00{v_max}.csv")
                    compra_df.to_csv(file_path)

                    messagebox.showinfo("Exito", f"Archivo csv creado client/OP_PT-Faltantes_00{v_max}.csv ")
                    self.parent.refresh_table()
                    self.destroy()
                
                except Exception as e:
                    titulo = "Error en funcion"
                    mensaje = f"Error: {str(e)}"
                    messagebox.showerror(titulo, mensaje)
                    print(e)
                    

        except Exception as e:
            titulo = "Error en funcion"
            mensaje = f"Error: {str(e)}"
            messagebox.showerror(titulo, mensaje)
            print(e)

class coti_remi_web_window(tk.Frame):
    def __init__(self, master, niv_user):
        super().__init__(master)

        self.niv_user = niv_user

        self.columnconfigure((0,1,2,3,4,5), weight = 1)
        # self.rowconfigure((0,1,2,3), weight = 1)

        self.folio_frame = tk.Frame(self)
        self.folio_frame.grid(row = 0, column = 0, padx = 10, pady = 10, sticky = "nw")

        self.tabla_frame = tk.Frame(self)
        self.tabla_frame.grid(row =1, column = 0)
        self.botones_frame = tk.Frame(self)
        self.botones_frame.grid(row = 1, column = 5, padx = 10, pady = 10, sticky = "nw")

        self.campos()
        self.tabla_web()

    def campos(self):
                #folio label
        self.folio_label = tk.Label(self.folio_frame, text = "Folio")
        self.folio_label.configure(font = ("Roboto", 12, "bold"), anchor = "e")
        self.folio_label.grid(row = 0, column = 0, sticky = "w")

        #entry folio
        self.my_folio = tk.StringVar()
        self.entry_folio = tk.Entry(self.folio_frame, textvariable = self.my_folio)
        self.entry_folio.configure(width = 15 , font = ("Roboto", 12))
        self.entry_folio.grid(row = 0, column = 1, sticky = "w")

        #boton buscar folio
        img_buscar_path = resource_path("img/lupa.png")
        img_buscar = Image.open(img_buscar_path)
        resized_img_buscar = img_buscar.resize((20,20), Image.Resampling.LANCZOS)
        self.converted_img_buscar = ImageTk.PhotoImage(resized_img_buscar)

        #buton buscar folio  
        self.boton_folio = ctk.CTkButton(self.folio_frame, text = "", image = self.converted_img_buscar, command=self.search_folio_pedweb)
        self.boton_folio.configure(width = 25, height = 25,
                                   border_width = 2)
        self.boton_folio.grid(row = 0, column = 2, padx = 5, sticky = "w")
    
     #detalle
        self.boton_detalle = ctk.CTkButton(self.botones_frame, text = "Consultar\nDetalles del Pedido", command = self.open_detalle)
        self.boton_detalle.configure(width = 200, height = 30,
                                   fg_color ="#3B8ED0", text_color="black", hover_color="#36719F",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_detalle.grid(row = 0, column = 0, pady = 10)


    #boton home 
    #img home
        img_home_path = resource_path("img/home.png")
        img_home = Image.open(img_home_path)
        resized_img_home = img_home.resize((35,35), Image.Resampling.LANCZOS)
        self.converted_img_home = ImageTk.PhotoImage(resized_img_home)

        #buton salir a home
        self.boton_salir = ctk.CTkButton(self.botones_frame, image = self.converted_img_home, text = "Home", command = self.open_main_window)
        self.boton_salir.configure(width = 200, height = 30,
                                   fg_color ="#FF5050", text_color="black", hover_color="#DD1818",
                                   border_width = 2,
                                   font = ("Roboto", 16, "bold"),
                                   compound  = tk.LEFT)
        self.boton_salir.grid(row = 4, column = 0, pady = 10)

    def tabla_web(self):
        #create table to show the entrys
        self.tabla = ttk.Treeview(self.tabla_frame,
                                  column = ("folio","tot_partidas", "documento", "cliente","clave_pedido","cant_total", "fecha", "almacen", "estatus"),#, "almacen", "estatus"),
                                  height = 30,
                                  show =  "headings"
                                  )
        # Add headings
        self.tabla.heading("folio", text="Folio")
        self.tabla.heading("tot_partidas", text="# de Partidas")
        self.tabla.heading("documento", text="Documento")
        self.tabla.heading("cliente", text="Cliente")
        self.tabla.heading("clave_pedido", text="Clave")
        self.tabla.heading("cant_total", text="Cant. Total ($)")
        self.tabla.heading("fecha", text="Fecha")
        self.tabla.heading("almacen", text="Almacen")
        self.tabla.heading("estatus", text="Estatus")

    # Configure column width
        self.tabla.column("folio", width=50)
        self.tabla.column("tot_partidas", width=100)
        self.tabla.column("documento", width=150)
        self.tabla.column("cliente", width=50)
        self.tabla.column("clave_pedido", width=100)
        self.tabla.column("cant_total", width=100) 
        self.tabla.column("fecha", width=100)
        self.tabla.column("almacen", width=100)
        self.tabla.column("estatus", width=100)

        self.tabla.tag_configure("pass", background = "#ACF5A7") #surtido
        self.tabla.tag_configure("SA", background = "#FF5247") #no surtido  

    
    # Pack the treeview inside the productos tab frame
        self.tabla.grid(row = 0, column = 0, columnspan = 9, padx=5, pady=5, sticky = "ew")

        #add scrollbar
        self.scroll = ttk.Scrollbar(self.tabla_frame,
                                    orient = "vertical", 
                                    command = self.tabla.yview)
        self.scroll.grid(row = 0, column = 10, sticky = "nse")
        self.tabla.configure(yscrollcommand = self.scroll.set)

        self.folio_coti_remi_web = self.my_folio.get()
        self.lista_web = select_ped_web(self.folio_coti_remi_web)
        self.lista_web.reverse()
        my_tag = "normal" #default tag

        for p in self.lista_web:
            my_tag = "pass" if p[8] != "SA" else "SA"
            self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))

    def search_folio_pedweb(self):

        self.folio_coti_remi_web = self.my_folio.get()

        if self.folio_coti_remi_web ==  "":
            self.tabla.delete(*self.tabla.get_children())
            self.lista_web = select_ped_web(self.folio_coti_remi_web)
            self.lista_web.reverse()

            for p in self.lista_web:
                my_tag = "pass" if p[8] != "SA" else "SA"
                self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))
        
        else:
            self.tabla.delete(*self.tabla.get_children())
            self.lista_web = select_ped_web(self.folio_coti_remi_web)
            self.lista_web.reverse()

            for p in self.lista_web:
                my_tag = "pass" if p[8] != "SA" else "SA"
                self.tabla.insert('', 0, text=p[0], values=(p[0:]), tags=(my_tag))
       
        
        # self.tabla.delete(*self.tabla.get_children())
        # self.lista_web = select_ped_web(self.folio_coti_remi_web)
        # self.lista_web.reverse()


    def open_detalle(self):
        try:
            selected_items = self.tabla.selection()
            items_df = []
            for item in selected_items:
                current_item = self.tabla.item(item)['values']
                items_df.append(current_item)
            items_df = pd.DataFrame(items_df, columns=["folio","tot_partidas", "documento", "cliente", "clave_pedido","cant_total","fecha",  "almacen", "estatus"]) 

            if(len(items_df)>1):
                messagebox.showerror("ERROR!", "Seleccionar solo una clave de documento")
            else:
                clave_documento = items_df['documento'][0]
                detalle_pedweb_window = detalle_pedweb(self.master, clave_documento)
                detalle_pedweb_window.pack(fill="both", expand=True)
        except:
            pass

    def open_main_window(self):
        hide_all_frames(self.master)
        niv_user = self.niv_user
        opciones_window = opcioneswindow(self.master, niv_user)
        opciones_window.pack(fill="both", expand=True)
    
class detalle_pedweb(tk.Toplevel):
    def __init__(self, parent, clave_documento):
        super().__init__(parent)
        self.clave_documento = clave_documento  # Store the DataFrame
        self.geometry("652x404")
        # self.resizable(0,0)
        self.columnconfigure((0, 1, 2), weight=1)
        self.rowconfigure((0, 1, 2, 3), weight=1)

        self.parent = parent

        self.title(f"Detalle del pedido {self.clave_documento}")

        # self.campos = tk.Frame(self)
        # self.campos.grid(row = 1, column = 0, columnspan=3)

        self.tabla_frame = tk.Frame(self)
        self.tabla_frame.grid(row = 0, column = 0, sticky='w')


        self.grab_set()
        self.tabla_web()

    def tabla_web(self):
        #create table to show the entrys
        self.tabla = ttk.Treeview(self.tabla_frame,
                                  column = ("folio", "documento", "skuvari", "cantidad", "precio", "costo"),#, "almacen", "estatus"),
                                  height = 15,
                                  show =  "headings"
                                  )
        # Add headings
        self.tabla.heading("folio", text="Folio")
        # self.tabla.heading("tot_partidas", text="# de Partidas")
        self.tabla.heading("documento", text="Documento")
        # self.tabla.heading("cliente", text="Cliente")
        # self.tabla.heading("clave_pedido", text="Clave")
        self.tabla.heading("skuvari", text="SKU")

        self.tabla.heading("cantidad", text="Cantidad")
        self.tabla.heading("precio", text="Precio ($)")
        self.tabla.heading("costo", text="Costo ($)")

    # Configure column width
        self.tabla.column("folio", width=50)
        self.tabla.column("documento", width=150)
        self.tabla.column("cantidad", width=100)
        self.tabla.column("skuvari", width=100)
        self.tabla.column("precio", width=100) 
        self.tabla.column("costo", width=100)

    
    # Pack the treeview inside the productos tab frame
        self.tabla.grid(row = 0, column = 0, columnspan = 6, padx=5, pady=5, sticky = "ew")

        #add scrollbar
        self.scroll = ttk.Scrollbar(self.tabla_frame,
                                    orient = "vertical", 
                                    command = self.tabla.yview)
        self.scroll.grid(row = 0, column = 6, sticky = "nse")
        self.tabla.configure(yscrollcommand = self.scroll.set)

        self.lista_web = select_detalle(self.clave_documento)
        self.lista_web.reverse()
       

        for p in self.lista_web:
            self.tabla.insert('', 0, text=p[0], values=(p[0:]))
        

def hide_all_frames(master):
    for widget in master.winfo_children():
        widget.pack_forget()


# Check if the script is being run directly by the Python interpreter
if __name__ == '__main__':
    main()
