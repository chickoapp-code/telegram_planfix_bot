#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Скрипт для поиска информации о customFieldData в swagger.json"""

import json
import sys
from pathlib import Path

swagger_path = Path(r"b:\БОТ ТП ТГ ПЛАНФИКС\swagger.json")

if not swagger_path.exists():
    print(f"Файл не найден: {swagger_path}")
    sys.exit(1)

print(f"Читаю {swagger_path}...")
with open(swagger_path, 'r', encoding='utf-8') as f:
    swagger = json.load(f)

# Ищем endpoint для создания задачи
print("\n" + "="*80)
print("Поиск endpoint для создания задачи (POST /task/)")
print("="*80)

paths = swagger.get('paths', {})
task_post = None
for path, methods in paths.items():
    if '/task' in path and 'post' in methods:
        task_post = methods.get('post', {})
        print(f"\nНайден: {path} (POST)")
        break

if task_post:
    # Ищем схему запроса
    request_body = task_post.get('requestBody', {})
    content = request_body.get('content', {})
    json_content = content.get('application/json', {})
    schema = json_content.get('schema', {})
    
    print("\nСхема запроса:")
    print(json.dumps(schema, indent=2, ensure_ascii=False))
    
    # Ищем customFieldData
    properties = schema.get('properties', {})
    if 'customFieldData' in properties:
        print("\n" + "="*80)
        print("НАЙДЕНО: customFieldData")
        print("="*80)
        custom_field_schema = properties['customFieldData']
        print(json.dumps(custom_field_schema, indent=2, ensure_ascii=False))
        
        # Ищем примеры
        if 'example' in custom_field_schema:
            print("\nПример:")
            print(json.dumps(custom_field_schema['example'], indent=2, ensure_ascii=False))
        
        # Ищем items схему для массива
        items = custom_field_schema.get('items', {})
        if items:
            print("\nСхема элемента массива:")
            print(json.dumps(items, indent=2, ensure_ascii=False))
            
            # Ищем схему value
            item_properties = items.get('properties', {})
            if 'value' in item_properties:
                value_schema = item_properties['value']
                print("\nСхема поля 'value':")
                print(json.dumps(value_schema, indent=2, ensure_ascii=False))
                
                # Ищем oneOf/anyOf для разных типов
                if 'oneOf' in value_schema:
                    print("\nВарианты формата 'value' (oneOf):")
                    for i, variant in enumerate(value_schema['oneOf']):
                        print(f"\nВариант {i+1}:")
                        print(json.dumps(variant, indent=2, ensure_ascii=False))
                
                if 'anyOf' in value_schema:
                    print("\nВарианты формата 'value' (anyOf):")
                    for i, variant in enumerate(value_schema['anyOf']):
                        print(f"\nВариант {i+1}:")
                        print(json.dumps(variant, indent=2, ensure_ascii=False))

# Ищем определения схем
print("\n" + "="*80)
print("Поиск определений схем (definitions/components)")
print("="*80)

components = swagger.get('components', {})
schemas = components.get('schemas', {})

# Ищем схемы связанные с customField
for name, schema_def in schemas.items():
    if 'custom' in name.lower() or 'field' in name.lower():
        print(f"\nНайдена схема: {name}")
        print(json.dumps(schema_def, indent=2, ensure_ascii=False)[:2000])  # Первые 2000 символов

# Ищем примеры в примерах запросов
print("\n" + "="*80)
print("Поиск примеров в swagger")
print("="*80)

def search_examples(obj, path=""):
    """Рекурсивный поиск примеров"""
    if isinstance(obj, dict):
        if 'example' in obj and 'customFieldData' in str(obj.get('example', '')):
            print(f"\nНайден пример в {path}:")
            print(json.dumps(obj['example'], indent=2, ensure_ascii=False))
        for key, value in obj.items():
            search_examples(value, f"{path}.{key}" if path else key)
    elif isinstance(obj, list):
        for i, item in enumerate(obj):
            search_examples(item, f"{path}[{i}]")

search_examples(swagger)

