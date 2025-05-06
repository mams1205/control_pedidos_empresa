# 🛒 Sistema de Gestión de Pedidos

Este repositorio incluye un **programa en Python** diseñado para gestionar los pedidos realizados tanto en la **tienda en línea** como en la **tienda física** de una empresa.

## 📌 Funcionalidades

- 📥 **Recolección de Pedidos**  
  Recopila automáticamente los pedidos realizados a través del sitio web y de la tienda física.

- 🗄️ **Integración con Base de Datos SQL**  
  Se conecta a una base de datos SQL donde se consolidan todos los pedidos para su procesamiento.

- 🧾 **Generación de Órdenes de Producción**  
  Crea las **órdenes de producción del día** en base a los pedidos recibidos.

- 📦 **Planificación de Materias Primas**  
  Extrae la lista de **materias primas faltantes** necesarias para completar los pedidos en curso.

## 🛠️ Tecnologías Utilizadas

- Python  
- SQL
- pandas
- numpy
- tkinter

## 📁 Estructura del Proyecto
├── eutrote_2.py # Script principal para ejecutar el proceso
├── conexion_db.py # Módulo para conexión con la base de datos
├── querys_dao.py # Funciones para gestionar y organizar pedidos


