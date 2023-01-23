# Trabalho Sistetmas Distribuidos 2022/2
São 5 arquivos que serão utilizados nas conexoes, são eles: client.py, adm.py, portal_client.py, portal_adm.py, bd.py
os arquivos portal_adm.py, portal_client.py e bd.py são scripts que ficam escutando o tempo inteiro em uma porta, então
precisam ser executados antes dos arquivos client.py e adm.py que servem como clientes.
uma correta ordem em terminais diferentes, seria:
py portal_adm.py
py portal_client.py
py bd.py
(A ordem entre esses nao importa, o importante é que sejam executados antes do client.py e adm.py)
py client.py
py adm.py
(A ordem entre o client.py e adm.py tbm nao tem relevancia)
depois disso, basta interagir com a interface no terminal

Além disso, é necessário criar manualmente os diretórios "dbOrder/5000", "dbOrder/5001", "dbOrder/5002", "dbProd/5000", "dbProd/5001", "dbProd/5002", "dbUser/5000", "dbUser/5001", "dbUser/5002". Esses diretórios serão usados pelo levelDB no uso das réplicas.

###############
Há 2 entradas para testes escritos em 2 arquivos, 1 deles é reservado para a interface do client e
1 deles para a interface do adm. São os arquivos: testadm, testclient
