#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Поиск информации о создании задачи и формате customFieldData"""

import json
from pathlib import Path

swagger_path = Path(r"b:\БОТ ТП ТГ ПЛАНФИКС\swagger.json")

with open(swagger_path, 'r', encoding='utf-8') as f:
    swagger = json.load(f)

# Ищем POST /task/ (создание задачи)
paths = swagger.get('paths', {})
task_create = None
for path, methods in paths.items():
    if path == '/task/' and 'post' in methods:
        task_create = methods.get('post', {})
        print(f"Найден: {path} (POST)")
        break

if task_create:
    print("\n" + "="*80)
    print("Схема запроса для POST /task/")
    print("="*80)
    
    request_body = task_create.get('requestBody', {})
    content = request_body.get('content', {})
    json_content = content.get('application/json', {})
    schema_ref = json_content.get('schema', {}).get('$ref', '')
    
    if schema_ref:
        # Извлекаем имя схемы из $ref
        schema_name = schema_ref.split('/')[-1]
        print(f"Схема: {schema_name}")
        
        # Ищем схему в components
        components = swagger.get('components', {})
        schemas = components.get('schemas', {})
        task_schema = schemas.get(schema_name, {})
        
        print("\nПолная схема:")
        print(json.dumps(task_schema, indent=2, ensure_ascii=False)[:5000])
        
        # Ищем customFieldData
        properties = task_schema.get('properties', {})
        if 'customFieldData' in properties:
            print("\n" + "="*80)
            print("customFieldData:")
            print("="*80)
            cf_schema = properties['customFieldData']
            print(json.dumps(cf_schema, indent=2, ensure_ascii=False))

# Ищем примеры для TaskCreateRequest или TaskRequest
components = swagger.get('components', {})
schemas = components.get('schemas', {})

print("\n" + "="*80)
print("Поиск схем Task*Request")
print("="*80)

for name in schemas.keys():
    if 'task' in name.lower() and 'request' in name.lower():
        print(f"\nНайдена схема: {name}")
        schema = schemas[name]
        print(json.dumps(schema, indent=2, ensure_ascii=False)[:3000])
        
        # Ищем примеры
        if 'x-examples' in schema:
            print("\nПримеры:")
            examples = schema['x-examples']
            for ex_name, ex_data in examples.items():
                print(f"\nПример '{ex_name}':")
                print(json.dumps(ex_data, indent=2, ensure_ascii=False)[:2000])

# Ищем примеры в paths
print("\n" + "="*80)
print("Поиск примеров в paths для /task/")
print("="*80)

for path, methods in paths.items():
    if '/task' in path:
        if 'post' in methods:
            post_method = methods['post']
            # Ищем примеры в requestBody
            request_body = post_method.get('requestBody', {})
            content = request_body.get('content', {})
            json_content = content.get('application/json', {})
            examples = json_content.get('examples', {})
            
            if examples:
                print(f"\nПримеры для {path} (POST):")
                for ex_name, ex_data in examples.items():
                    print(f"\nПример '{ex_name}':")
                    ex_value = ex_data.get('value', {})
                    if 'customFieldData' in str(ex_value):
                        print(json.dumps(ex_value, indent=2, ensure_ascii=False))

