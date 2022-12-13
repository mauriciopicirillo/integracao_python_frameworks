# Importações

from time import sleep
from sqlalchemy import create_engine
from sqlalchemy import inspect
from sqlalchemy import Column
from sqlalchemy import Integer
from sqlalchemy import String
from sqlalchemy import ForeignKey
from sqlalchemy import select
from sqlalchemy import func
from sqlalchemy.orm import declarative_base
from sqlalchemy.orm import relationship
from sqlalchemy.orm import Session

# Criando classe base para usar a propriedade de herança

Base = declarative_base()


class Cliente(Base):
    """Classe/Tabela Cliente: Atributos: [id, nome, cpf, endereco]"""

    __tablename__ = "cliente"
    id = Column(Integer, primary_key=True)
    nome = Column(String(50), nullable=False)
    cpf = Column(String(9), nullable=False, unique=True)
    endereco = Column(String(75), nullable=False)

    # Definindo relacionamento com a tabela de contas

    conta = relationship("Conta", back_populates="cliente", cascade="all, delete-orphan")

    # Representação da classe/tabela

    def __repr__(self):
        return f"Cliente(id={self.id}, nome={self.nome}, cpf={self.cpf}, address={self.endereco})"


class Conta(Base):
    """Classe/Tabela Conta: Atributos: [id, conta_tipo, agencia, numero_conta, cliente_id]"""

    __tablename__ = "conta"
    id = Column(Integer, primary_key=True)
    conta_tipo = Column(String, nullable=False)
    agencia = Column(Integer, default="OOO1")
    numero_conta = Column(Integer, unique=True, nullable=False)
    cliente_id = Column(Integer, ForeignKey("client.id"), nullable=False) #Chave estrangeira para a tabela de usuários

    # Definindo relacionamento com a tabela de usuários

    cliente = relationship("Cliente", back_populates="conta")

    # Representação da classe/tabela

    def __repr__(self):
        return f"Conta(id={self.id}, conta_tipo={self.conta_tipo}, agencia={self.agencia}, numero_conta={self.numero_conta}, cliente_id={self.cliente_id})"


# Fazendo conexão com o banco de dados:

engine = create_engine("sqlite://")

# Instanciação das classes como tabelas no banco de dados:

Base.metadata.create_all(engine)


class SQL:
    """Classe para comandos SQL"""

    def __init__(self, engine):
        self.engine = engine
        self.session = Session(self.engine) # Iniciando sessão
        self.numero_conta = self.numero_conta_conta() # Chamando o método para definir o ínicio da contagem de contas

    # Inserção de registros na tabelas

    def inserir(self, name, cpf, endereco, account_type):
        self.numero_conta += 1
        register = Cliente(name=nome, cpf=cpf, endereco=endereco,
                          conta=[Conta(conta_tipo=conta_tipo, numero_conta=self.numero_conta)])
        with self.session:
            self.session.add(register)
            self.session.commit()

    # Inserção de registros na tabelas possibilitando o usuário ter dois tipos de contas

    def inserir_2(self, nome, cpf, endereco, conta_tipo1, conta_tipo2):
        self.numero_conta += 1
        register = Cliente(nome=nome, cpf=cpf, endereco=endereco,
                          conta=[Conta(conta_tipo=conta_tipo1, numero_conta=self.numero_conta),
                                   Conta(conta_tipo=conta_tipo2, numero_conta=self.numero_conta+1)])
        with self.session:
            self.session.add(register)
            self.session.commit()

    # Método para retornar o número de contas existentes no banco

    def numeros_de_contas(self):
        statement = select(func.count('*')).select_from(Conta)
        for query in self.session.scalars(statement):
            return query

    # Método para retornar um determinado cliente por cpf

    def cliente_por_cpf(self, cpf):
        statement = select(Cliente).where(Cliente.cpf.in_([cpf]))
        for query in self.session.scalars(statement):
            return query

    # Método para retornar uma determinada conta pelo cpf de um cliente

    def conta_por_cpf(self, cpf):
        pre_statement = select(Cliente.id).where(Cliente.cpf.in_([cpf]))
        for pre_query in self.session.scalars(pre_statement):
            pre_query = pre_query
        end_statement = select(Conta).where(Conta.client_id.in_([pre_query]))
        for end_query in self.session.scalars(end_statement):
            return end_query

    # Método para retornar todos os registros do banco

    def todos_registros(self):
        connection = self.engine.connect()
        results = connection.execute(select(Cliente, Conta).join_from(Conta, Cliente)).fetchall()
        for query in results:
            return query

    # Método para retornar todos os clientes do banco ordenados alfabeticamente do maior para o menor

    def clientes_por_nome_em_ordem_descrescente(self):
        statement = select(Cliente).order_by(Cliente.name.desc())
        for query in self.session.scalars(statement):
            return query


# Função do Menu

def menu(SQL):
    inspector_engine = inspect(engine)
    print("\nBem vindo!\n")
    sleep(2)
    print(" Sistema de Banco de Dados ".center(31, '#'))
    print()
    op = None
    op_ver = ['1', '2', '3', '4', '5']
    sleep(2)
    print(" Menu ".center(30, '='))
    while op != 5:
        print(" [1] Mostrar Tabelas \n [2] Inserir Cliente\n [3] Procurar Cliente \n [4] Mostrar Registros \n [5] Encerrar Programa")
        op = input('Opção: ')
        while op not in op_ver:
            print("Por favor insira uma opção válida!")
            op = input('Opção: ')
        match int(op):
            case 1:
                print()
                print(inspector_engine.get_table_names())
            case 2:
                print()
                try:
                    name = input("Insira o seu nome: ")
                    while name.isnumeric() is True or name.isspace() is True or name == "":
                        print("Por favor insira um nome válido!")
                        name = str(input("Insira o seu nome completo: "))
                    name = name.strip().title()
                    cpf = input("Insira o seu cpf: ").replace(",", "").replace(".", "").replace("-", "").strip()
                    aux = str(cpf)
                    while aux == "" or len(list(aux)) != 9:
                        print("Por favor insira um cpf válido!")
                        cpf = input("Insira o seu cpf: ").replace(",", "").replace(".", "").replace("-", "").strip()
                        aux = str(cpf)
                    address = input("Insira o seu endereço: ").strip()
                    while address == "":
                        print("Por favor insira um endereço válido!")
                        address = input("Insira o seu endereço: ").strip()
                    print("Qual tipo de conta você deseja:\n[0] Conta Corrente\n[1] Conta Poupança\n[2] Conta Corrente e Poupança ")
                    o = input(": ")
                    o_ver = ['0', '1', '2']
                    while o not in o_ver:
                        print("Por favor insira uma opção válida!")
                        o = input(": ")
                    match int(o):
                        case 0:
                            account_type = "Conta Corrente"
                            SQL.insert(name, cpf, address, account_type)
                        case 1:
                            account_type = "Conta Poupança"
                            SQL.insert(name, cpf, address, account_type)
                        case 2:
                            account_type1 = "Conta Corrente"
                            account_type2 = "Conta Poupança"
                            SQL.insert_2(name, cpf, address, account_type1, account_type2)
                    print("Cliente inserido com sucesso!\n")
                except:
                    print("\nErro na inserção, por favor insira informações válidas, não aceitamos valores nulos e nem o mesmo CPF para dois cadastros\n")
            case 3:
                print()
                if SQL.account_number == 0:
                    print("None")
                else:
                    cpf = input("Insira o cpf do cliente: ")
                    if SQL.select_client(cpf) is None:
                        print("O CPF procurado não existe\n")
                    else:
                        print("\nCliente:")
                        print(SQL.select_client(cpf))
                        print("\nConta:")
                        print(SQL.select_client_account(cpf))
                        print()
            case 4:
                print()
                print(SQL.select_all())
                print()
            case 5:
                print("Encerrando...")
                sleep(1)
                break
    print("Volte Sempre!")


# Rodando o código

if __name__ == '__main__':
    SQL = SQL(engine)
    menu(SQL)