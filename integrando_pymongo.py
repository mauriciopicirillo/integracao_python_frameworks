# Importações

import pymongo
import datetime
import pprint

# Realizando conexão com o pymongo


cliente = pymongo.MongoClient("mongodb+srv://ditinhojack:<password>@cluster0.5jpooux.mongodb.net/?retryWrites=true&w=majority")

# Definindo nome do database

db = cliente.pymongo

# Bulk Insert - Contas dos Clientes

novas_contas = [{
    "cliente_nome": "Mauricio Picirillo",
    "pessoa": "Fisica",
    "conta_tipo": "Conta Poupança",
    "agencia": "0001",
    "numero_conta": 1,
},
    {
        "cliente_nome": "Caio Picirillo",
        "pessoa": "Juridica",
        "conta_tipo": "Conta Corrente",
        "agencia": "0001",
        "numero_conta": 2,
    },
    {
        "cliente_nome": "Juliana Soares",
        "pessoa": "Juridica",
        "conta_tipo": "Conta Corrente",
        "agencia": "0001",
        "numero_conta": 3,
    },
    {
        "cliente_nome": "Bianca Soares",
        "pessoa": "Fisica",
        "conta_tipo": "Conta Poupança",
        "agencia": "0001",
        "numero_conta": 4,
    }
]

contas = db.contas
contas.insert_many(novas_contas)

# Bulk Insert - Inserindo operações realizadas pelos clientes

novas_operacoes = [{
    "cliente_id": f"{db.contas.find_one({'numero_conta': 1})}", # "Foreign Key" para a conta do cliente
    "tipo_operacao": "Deposito",
    "valor": 50.00,
    "sucesso_operacao": True,
    "data": datetime.datetime.utcnow()
},
    {
        "cliente_id": f"{db.contas.find_one({'numero_conta': 2})}",
        "tipo_operacao": "Saque",
        "valor": 25.20,
        "sucesso_operacao": False,
        "data": datetime.datetime.utcnow()
    },
    {
        "cliente_id": f"{db.contas.find_one({'numero_conta': 1})}",
        "tipo_operacao": "Pix Recebido",
        "valor": 105.63,
        "sucesso_operacao": True,
        "data": datetime.datetime.utcnow()
    },
    {
        "cliente_id": f"{db.contas.find_one({'numero_conta': 2})}",
        "tipo_operacao": "Pix Enviado",
        "valor": 99.50,
        "sucesso_operacao": True,
        "data": datetime.datetime.utcnow()
    }
]

operacoes = db.operacoes
operacoes.insert_many(novas_operacoes)

# Realizando um join entre o documento de conta dos clientes e suas operações realizadas

operacoes_contas = db.operacoes.aggregate([{'$lookup':
                                     {'from': 'operacoes',
                                      'localField': '_id',
                                      'foreignField': 'cliente_id', 'as': 'fromContas'}
                                 },
                                {'$replaceRoot':
                                     {'newRoot':
                                          {'$mergeObjects':
                                               [{'$arrayElemAt': ['$fromContas', 0]}, '$$ROOT']}}},
                                {'$project': {'fromContas': 0}}])

print("\nClientes e suas operações:\n")
for result in operacoes_contas:
    print(f"{result}\n")

#Recuperando todas as contas

print("\nContas:")
for result in contas.find():
    pprint.pprint(result)