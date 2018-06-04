# Segundo Projeto de Sistemas Distribuídos

![alt text](http://asc.di.fct.unl.pt/sd/labs/tp2/tp2-updated-architecture.png)

## Instalação
	
	actualização do pom : Personalizar SD18-TP2-XXXXX-YYYYY
		-Substituir XXXXX-YYYYY (que será o nome da imagem)

	script : testn-sd2.sh

	script-services: sd18_services.sh

	Meter script executável: chmod a+x testn-sd2.sh

	Meter script executavel: chmod a+x sd18_services.sh

	Fazer mvn clean install
	
	Primeiro Services : ./sd18_services.sh

	Segundo : ./testn-sd2.sh -image <img> <options>

	<options>

	-test <num>, eg., -test 3c, para executar os testes 3c em diante;
	-log OFF, -log FINE ou -log ALL, para controlar a quantidade de output;
	-sleep <seconds>, para controlar o tempo de espera entre o lançamento dos servidores e a execução dos testes.

## Objetivo

Objetivo do trabalho consiste em desenvolver um sistema de processamento distribuído de dados textuais baseado no paradigma MapReduce.

O objetivo principal deste trabalho é tornar o sistema desenvolvido no trabalho anterior num sistema confiável (dependable). Face a modelos de falhas e segurança atualizados e mais realistas, pretende-se obter um sistema que ofereça elevada disponibilidade e melhores garantias de segurança, evitando que agentes não autorizados possam interferir no seu funcionamento.

O sistema de armazenamento será ainda dotado da capacidade de usar repositórios de dados externos, tais como a Dropbox.



## Armazenamento Externo Dropbox
	
	Armazenar os Blocos externamente

	Recorrendo:

	-CREATE = "https://content.dropboxapi.com/2/files/upload";

	-GET = "https://content.dropboxapi.com/2/files/download";
	
	-DELETE = "https://api.dropboxapi.com/2/files/delete_v2";

	-LIST_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/list_folder";

	-LIST_FOLDER_CONTINUE_V2_URL = "https://api.dropboxapi.com/2/files/list_folder/continue";

	-CREATE_FOLDER_V2_URL = "https://api.dropboxapi.com/2/files/create_folder_v2";

## Segurança e Novas Funcionalidades

Autenticidade de componentes e comunicações seguras;

Suporte de WebServices REST e SOAP sobre TLS.

Interação com serviço externo, acessível por REST e com autenticação e controlo de acessos OAuth.

## Disponibilidade (*)

Componentes tolerantes a falhas, por recurso a serviços externos;

Recurso à MongoDB para armazenamento e pesquisa de metadados;

Escritas redundantes dos dados (blocos).

## Integridade dos Dados

1. Usar classe Hash juntamente com a classe Base58.java, para implementar a verificação de integridade dos dados armazenados pelo Datanode.

2. Caso um bloco seja detetado com estando corrompido, o Datanode deverá assinalar esse facto.

  2.1 enviando como resposta à leitura um bloco contendo os bytes da string: <<<CORRUPTED BLOCK>>>

3. A validação desta funcionalidade pelo programa de teste requer que se indique no ficheiro .props qual a pasta onde são guardados os blocos.
	
(NOTA) A fiabilidade deste teste irá depender da capacidade do programa de teste em corromper blocos...

## Autenticação TLS do Cliente

O modelo usual de acesso a recursos http e https apenas envolve a autenticação do servidor.

Podendo o cliente ser anónimo. 

Quando é necessário autenticar o cliente, é frequente usar-se mecanismos baseados em segredos partilhados. 

Neste modelo, o cliente confunde-se com o utilizador, sendo este último responsável por manter os segredos partilhados seguros do seu lado. 

Porém, o TLS oferece a possibilidade de autenticar o cliente, algo especialmente útil quando o funcionamente do cliente não envolve intervenção humana. 

Para tal, é necessário configurar o contexto SSL do servidor para que este solicite no estabelecimento da ligação, que o cliente envie o seu certificado. 

A maneira de conseguir obter este comportamento do servidor não é imediatamente óbvia, em particular quando consiste apenas em colocar a true um parâmetro da configuração do servidor.

# Correction

	URI Mongo : mongodb://mongo1,mongo2,mongo3/?w=majority&readConcernLevel=majority&readPreference=secondary

	

