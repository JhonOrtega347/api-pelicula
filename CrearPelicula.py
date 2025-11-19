
import boto3
import uuid
import os
import json
import traceback
from botocore.exceptions import ClientError

def lambda_handler(event, context):
    try:
        # Entrada (json) - log INFO
        print(json.dumps({
            'tipo': 'INFO',
            'log_datos': {
                'event': event
            }
        }, ensure_ascii=False))
        tenant_id = event['body']['tenant_id']
        pelicula_datos = event['body']['pelicula_datos']
        nombre_tabla = os.environ["TABLE_NAME"]

        # Proceso
        uuidv4 = str(uuid.uuid4())
        pelicula = {
            'tenant_id': tenant_id,
            'uuid': uuidv4,
            'pelicula_datos': pelicula_datos
        }
        dynamodb = boto3.resource('dynamodb')
        table = dynamodb.Table(nombre_tabla)
        response = table.put_item(Item=pelicula)

        # Salida (json) - log INFO
        print(json.dumps({
            'tipo': 'INFO',
            'log_datos': {
                'pelicula': pelicula
            }
        }, ensure_ascii=False))
        return {
            'statusCode': 200,
            'pelicula': pelicula,
            'response': response
        }

    except KeyError as e:
        # Datos de entrada incompletos
        error_msg = f"Missing key in input: {e}"
        print(json.dumps({
            'tipo': 'ERROR',
            'log_datos': {
                'message': error_msg,
                'missing_key': str(e)
            }
        }, ensure_ascii=False))
        return {
            'statusCode': 400,
            'error': error_msg
        }

    except ClientError as e:
        # Errores del cliente de AWS (DynamoDB, permisos, etc.)
        error_msg = f"DynamoDB client error: {e}"
        # Try to include response metadata if available
        details = None
        try:
            details = e.response
        except Exception:
            details = str(e)
        print(json.dumps({
            'tipo': 'ERROR',
            'log_datos': {
                'message': 'DynamoDB client error',
                'error': str(e),
                'response': details
            }
        }, ensure_ascii=False))
        return {
            'statusCode': 502,
            'error': 'DynamoDB client error',
            'details': str(e)
        }

    except Exception as e:
        # Excepción genérica
        error_msg = f"Unhandled exception: {e}"
        tb = traceback.format_exc()
        print(json.dumps({
            'tipo': 'ERROR',
            'log_datos': {
                'message': error_msg,
                'error': str(e),
                'traceback': tb
            }
        }, ensure_ascii=False))
        return {
            'statusCode': 500,
            'error': 'Internal server error',
            'details': str(e)
        }
