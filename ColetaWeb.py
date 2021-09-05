'''
Coleta dos dados da API da Câmara dos Deputados

Coleta das votações e do posicionamento dos partidos em relação
as votações realizada no período 02/02/2021 a 30/08/2021.
'''

# Bibliotecas utilizadas no projeto
import lxml
from bs4 import BeautifulSoup
import pandas as pd
import requests
import json
from datetime import datetime, timedelta


#Função para coleta das votações
def coletar(inicio,fim,npag): # É necessário informar a data inicial, a data final e o número de páginas que deseja coletar
    cont =1
    votacoes = pd.DataFrame() # Criação do data frame que receberá os dados
    while cont != npag+1:  # Essa função funciona da seguinte forma: se o contador for diferente do número de páginas + 1, ele para. Então coletará da primeira página até a página n que o usuário deseja e irá parar
        for _ in range(3):
            try:
                ap = requests.get(f'https://dadosabertos.camara.leg.br/api/v2/votacoes?dataInicio={inicio}&dataFim={fim}&pagina={cont}') #Conecta com a API
                ap = BeautifulSoup(ap.text,'lxml') # XML da página
                ap = json.loads(ap.find('p').text) # Coleta o json que contém os dados
                ap = ap['dados'] #Retorna os dados da API
            except: 
                print('Error')


        print('Página ',cont , ' de ' , npag)
        cont +=1 # Soma 1 no contador
        for audi in range(0,len(ap)): # Percorre todos os dados da API
            aud = ap[audi] 
            a_id = aud['id'] # Coleta o ID
            ap1 = requests.get(f'https://dadosabertos.camara.leg.br/api/v2/votacoes/{a_id}/votos')  # Conecta com a API aonde se encontra os votos dos deputados
            ap1 = BeautifulSoup(ap1.text,'lxml') #XML da página
            ap1 = json.loads(ap1.find('p').text) # Coleta o json que contém os dados
            ap1 = ap1['dados'] # Retorna os dados da API
            if ap1 == []: # Se não tiver dados referente aos votos, a função não coleta os dados da votação.
                pass
            else: # Quando existe dados sobre os votos
                print(audi)
                vot = requests.get(f'https://dadosabertos.camara.leg.br/api/v2/votacoes/{a_id}') # Conecta com API aonde se encontra dados sobre as votações
                #print(f'https://dadosabertos.camara.leg.br/api/v2/votacoes/{a_id}')
                vot = BeautifulSoup(vot.text,'lxml') #XML da página
                vot = json.loads(vot.find('p').text) # Coleta o json que contém os dados
                vot = vot['dados'] #Retorna os dados da API

                n= len(ap1) # Quantidade de votos
                data = vot['data'] # Data da votação
                siglaOrgao = vot['siglaOrgao'] # Sigla do Orgão da votação
                aprovacao = vot['aprovacao'] # Se foi aprovado ou não
                if aprovacao == None: # Se não tiver dados sobre aprovação, retorna ''.
                    aprovacao =''
                descricao =  vot['efeitosRegistrados']# Descrição da votação
                try:
                    descricao =vot['efeitosRegistrados'][0]['descUltimaApresentacaoProposicao']# Descrição da votação
                except: 
                    try:
                        descricao =  vot['ultimaApresentacaoProposicao']['descricao']# Descrição da votação
                    except:
                        try:
                            descricao =  vot['proposicoesAfetadas'][0]['ementa']# Descrição da votação
                        except:
                            descricao = '' #Se não tiver dados sobre a descrição, retorna ''
                if type(descricao) == 'list' and descricao >0: # Se a variável Descrição for uma lista, coleta o último elemento da lista.
                    descricao = descricao[-1]


                ### Coletando dados sobre os votos
                tipoVoto, nome_dep, sigla_partido_dep =[],[],[] #criando listas para receber dados sobre os votos
                sigla_uf_dep, idLegislatura, urlFoto, email_dep = [],[],[],[] #criando listas para receber dados sobre os votos
                for audi1 in range(0,len(ap1)): #Percorre os dados sobre os votos.
                    aud1= ap1[audi1]
                    tipoVoto.append(aud1['tipoVoto']) #Coleta o tipo do Voto
                    nome_dep.append(aud1['deputado_']['nome'])#Coleta o nome do deputado
                    sigla_partido_dep.append(aud1['deputado_']['siglaPartido'])#Coleta a sigla do partido do deputado
                    sigla_uf_dep.append(aud1['deputado_']['siglaUf'])#Coleta a UF do deputado
                    idLegislatura.append(aud1['deputado_']['idLegislatura'])#Coleta o ID de Legislatura do deputado
                    urlFoto.append(aud1['deputado_']['urlFoto'])#Coleta a foto do deputado
                    email_dep.append(aud1['deputado_']['email'] )#Coleta o email do deputado


                ### Salva dados no dataframe
                votacoes = votacoes.append(pd.DataFrame({'ID':[a_id]*len(ap1),'Data':[data]*len(ap1),'Sigla_Orgao':[siglaOrgao]*len(ap1),
                                                        'Aprovacao':[aprovacao]*len(ap1),'Descricao':[descricao]*len(ap1),
                                                        'tipoVoto':tipoVoto,'nome_dep':nome_dep,
                                                        'sigla_partido_dep':sigla_partido_dep,
                                                        'sigla_uf_dep':sigla_uf_dep, 'idLegislatura':idLegislatura,
                                                        'urlFoto':urlFoto,'email_dep':email_dep}))
    return votacoes


df1 = coletar('2021-06-01','2021-08-30',9) #Usa a função para coletar do mês 06 ao mês 08 de 2021
df2 = coletar('2021-02-02','2021-05-31',13)#Usa a função para coletar do mês 02 ao mês 05 de 2021


votacoes = pd.concat([df1,df2]).reset_index() # Junta as bases de dados
votacoes.info()


### Transformação dos dados 
votacoes['Aprovacao']= votacoes['Aprovacao'].map({0:'Não',1:'Sim'}) # Mudança de dados na variável Aprovacao
votacoes['Descricao']=votacoes['Descricao'].fillna('Não Informado') # Preenchendo valores nulos 
votacoes['Sim'] = np.where(votacoes['tipoVoto'] == 'Sim',1,0) # Coluna dos deputados que votaram sim
votacoes['Não'] = np.where(votacoes['tipoVoto'] == 'Não',1,0) # Coluna dos deputados que votaram não


ID = list(votacoes["ID"].unique()) # lista dos ID's das votações

# Função para coleta da posição do partido 
def coleta_posicao(id): # É necessário informar a lista de ID
    partido = pd.DataFrame()
    for i in range(0,len(id)): # Percorre os ID's
        print(i, ' de ', len(id)-1)
        ap = requests.get(f'https://dadosabertos.camara.leg.br/api/v2/votacoes/{id[i]}/orientacoes') #Conecta com a API de orientações dos partidos
        ap = BeautifulSoup(ap.text,'lxml') #XML da página
        ap = json.loads(ap.find('p').text) # Coleta o json que contém os dados
        ap = ap['dados'] # Retorna dados da api
        if ap == []: # Se não tiver dados, a função não coleta nada.
            pass
        else:
            for audi in range(0,len(ap)): # Percorre a posição de cada partido
                aud = ap[audi] 
                orientacaoVoto = aud['orientacaoVoto'] # coleta a orientação do voto
                siglaPartidoBloco = aud['siglaPartidoBloco'] # Coleta a sigla do partido


                ### Salva dados no dataframe
                partido = partido.append(pd.DataFrame({"ID":id[i],"orientacaoVoto":orientacaoVoto,
                                                      "siglaPartidoBloco":siglaPartidoBloco},index=[0]))
    return partido



partido = coleta_posicao(ID) # Usa função para coletar dados dos partidos


# Merge entre as bases de dados
new_df = pd.merge(votacoes, partido,  how='left', left_on=['ID','sigla_partido_dep'], right_on = ['ID','siglaPartidoBloco'])
new_df['orientacaoVoto']=new_df['orientacaoVoto'].fillna('Não Informado')

new_df.head()

### Salvando CSV
new_df.to_csv('Coleta.csv',sep=';')


